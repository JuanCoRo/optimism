package processors

import (
	"context"
	"fmt"
	"math/big"

	"gorm.io/gorm"

	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/indexer/bigint"
	"github.com/ethereum-optimism/optimism/indexer/config"
	"github.com/ethereum-optimism/optimism/indexer/database"
	"github.com/ethereum-optimism/optimism/indexer/etl"
	"github.com/ethereum-optimism/optimism/indexer/processors/bridge"
	"github.com/ethereum-optimism/optimism/op-service/tasks"
)

var blocksLimit = 10_000

type BridgeProcessor struct {
	log     log.Logger
	db      *database.DB
	metrics bridge.Metricer

	resourceCtx    context.Context
	resourceCancel context.CancelFunc
	tasks          tasks.Group

	l1Etl       *etl.L1ETL
	l2Etl       *etl.L2ETL
	chainConfig config.ChainConfig

	LastL1Header *database.L1BlockHeader
	LastL2Header *database.L2BlockHeader

	LastFinalizedL1Header *database.L1BlockHeader
	LastFinalizedL2Header *database.L2BlockHeader
}

func NewBridgeProcessor(log log.Logger, db *database.DB, metrics bridge.Metricer, l1Etl *etl.L1ETL, l2Etl *etl.L2ETL,
	chainConfig config.ChainConfig, shutdown context.CancelCauseFunc) (*BridgeProcessor, error) {
	log = log.New("processor", "bridge")

	latestL1Header, err := db.BridgeTransactions.L1LatestBlockHeader()
	if err != nil {
		return nil, err
	}
	latestL2Header, err := db.BridgeTransactions.L2LatestBlockHeader()
	if err != nil {
		return nil, err
	}

	latestFinalizedL1Header, err := db.BridgeTransactions.L1LatestFinalizedBlockHeader()
	if err != nil {
		return nil, err
	}
	latestFinalizedL2Header, err := db.BridgeTransactions.L2LatestFinalizedBlockHeader()
	if err != nil {
		return nil, err
	}

	log.Info("detected indexed bridge state",
		"l1_block", latestL1Header, "l2_block", latestL2Header,
		"finalized_l1_block", latestFinalizedL1Header, "finalized_l2_block", latestFinalizedL2Header)

	resCtx, resCancel := context.WithCancel(context.Background())
	return &BridgeProcessor{
		log:                   log,
		db:                    db,
		metrics:               metrics,
		l1Etl:                 l1Etl,
		l2Etl:                 l2Etl,
		resourceCtx:           resCtx,
		resourceCancel:        resCancel,
		chainConfig:           chainConfig,
		LastL1Header:          latestL1Header,
		LastL2Header:          latestL2Header,
		LastFinalizedL1Header: latestFinalizedL1Header,
		LastFinalizedL2Header: latestFinalizedL2Header,
		tasks: tasks.Group{HandleCrit: func(err error) {
			shutdown(fmt.Errorf("critical error in bridge processor: %w", err))
		}},
	}, nil
}

func (b *BridgeProcessor) Start() error {
	b.log.Info("starting bridge processor...")

	l1EtlUpdates := b.l1Etl.Notify()
	l2EtlUpdates := b.l2Etl.Notify()

	b.tasks.Go(func() error {
		for {
			select {
			case <-b.resourceCtx.Done():
				b.log.Info("stopping bridge processor")
				return nil

			// Tickers
			case <-l1EtlUpdates:
				done := b.metrics.RecordInterval()
				done(b.onL1Data(context.Background()))
			case <-l2EtlUpdates:
				done := b.metrics.RecordInterval()
				done(b.onL2Data(context.Background()))
			}
		}
	})
	return nil
}

func (b *BridgeProcessor) Close() error {
	// signal that we can stop any ongoing work
	b.resourceCancel()
	// await the work to stop
	return b.tasks.Wait()
}

func (b *BridgeProcessor) onL1Data(ctx context.Context) error {
	b.log.Info("notified of new L1 state", "etl_block_number", b.l1Etl.LatestHeader.Number)
	_ = b.processInitiatedL1Events(ctx)
	_ = b.processFinalizedL2Events(ctx)
	return nil
}

func (b *BridgeProcessor) onL2Data(ctx context.Context) error {
	b.log.Info("notified of new L2 state", "etl_block_number", b.l2Etl.LatestHeader.Number)
	_ = b.processInitiatedL2Events(ctx)
	_ = b.processFinalizedL1Events(ctx)
	return nil
}

func (b *BridgeProcessor) processInitiatedL1Events(ctx context.Context) error {
	l1BridgeLog := b.log.New("bridge", "l1", "kind", "initiated")
	lastL1BlockNumber := big.NewInt(int64(b.chainConfig.L1StartingHeight))
	if b.LastL1Header != nil {
		lastL1BlockNumber = b.LastL1Header.Number
	}

	// Latest unobserved L1 state bounded by `blockLimits` blocks. NewDB allows for the creation of a fresh subquery stmt.
	latestL1HeaderScope := func(db *gorm.DB) *gorm.DB {
		return db.Where("number = (?)", db.Session(&gorm.Session{NewDB: true}).Model(database.L1BlockHeader{}).
			Select("MAX(number) as number").Order("number ASC").Limit(blocksLimit).Where("number > ?", lastL1BlockNumber))
	}

	latestL1Header, err := b.db.Blocks.L1BlockHeaderWithScope(latestL1HeaderScope)
	if err != nil {
		return fmt.Errorf("failed to query new L1 state: %w", err)
	} else if latestL1Header == nil {
		return fmt.Errorf("no new L1 state found")
	}

	fromL1Height, toL1Height := new(big.Int).Add(lastL1BlockNumber, bigint.One), latestL1Header.Number
	l1BridgeLog.Info("unobserved block range", "from_block_number", fromL1Height, "to_block_number", toL1Height)
	if err := b.db.Transaction(func(tx *database.DB) error {
		l1BedrockStartingHeight := big.NewInt(int64(b.chainConfig.L1BedrockStartingHeight))
		if l1BedrockStartingHeight.Cmp(fromL1Height) > 0 { // OP Mainnet & OP Goerli Only.
			legacyFromL1Height, legacyToL1Height := fromL1Height, toL1Height
			if l1BedrockStartingHeight.Cmp(toL1Height) <= 0 {
				legacyToL1Height = new(big.Int).Sub(l1BedrockStartingHeight, bigint.One)
			}

			legacyBridgeLog := l1BridgeLog.New("mode", "legacy", "from_block_number", legacyFromL1Height, "to_block_number", legacyToL1Height)
			l1BridgeLog.Info("scanning for legacy initiated bridge events")
			if err := bridge.LegacyL1ProcessInitiatedBridgeEvents(l1BridgeLog, tx, b.metrics, b.chainConfig.L1Contracts, legacyFromL1Height, legacyToL1Height); err != nil {
				return err
			} else if legacyToL1Height.Cmp(toL1Height) == 0 {
				return nil // a-ok! Entire range was legacy blocks
			}
			legacyBridgeLog.Info("detected switch to bedrock")
			fromL1Height = l1BedrockStartingHeight
		}

		l1BridgeLog = l1BridgeLog.New("from_block_number", fromL1Height, "to_block_number", toL1Height)
		l1BridgeLog.Info("scanning for initiated bridge events")
		return bridge.L1ProcessInitiatedBridgeEvents(l1BridgeLog, tx, b.metrics, b.chainConfig.L1Contracts, fromL1Height, toL1Height)
	}); err != nil {
		return err
	}

	b.LastL1Header = latestL1Header
	return nil
}

func (b *BridgeProcessor) processInitiatedL2Events(ctx context.Context) error {
	l2BridgeLog := b.log.New("bridge", "l2", "kind", "initiated")
	lastL2BlockNumber := bigint.Zero // skipping genesis
	if b.LastL2Header != nil {
		lastL2BlockNumber = b.LastL2Header.Number
	}

	// Latest unvisited L2 state bounded by `blockLimits` blocks. NewDB allows for the creation of a fresh subquery stmt.
	latestL2HeaderScope := func(db *gorm.DB) *gorm.DB {
		return db.Where("number = (?)", db.Session(&gorm.Session{NewDB: true}).Model(database.L2BlockHeader{}).
			Select("MAX(number) as number").Order("number ASC").Limit(blocksLimit).Where("number > ?", lastL2BlockNumber))
	}

	latestL2Header, err := b.db.Blocks.L2BlockHeaderWithScope(latestL2HeaderScope)
	if err != nil {
		return fmt.Errorf("failed to query new L2 state: %w", err)
	} else if latestL2Header == nil {
		return fmt.Errorf("no new L2 state found")
	}

	fromL2Height, toL2Height := new(big.Int).Add(lastL2BlockNumber, bigint.One), latestL2Header.Number
	l2BridgeLog.Info("unobserved block range", "from_block_number", fromL2Height, "to_block_number", toL2Height)
	if err := b.db.Transaction(func(tx *database.DB) error {
		l2BedrockStartingHeight := big.NewInt(int64(b.chainConfig.L2BedrockStartingHeight))
		if l2BedrockStartingHeight.Cmp(fromL2Height) > 0 { // OP Mainnet & OP Goerli Only
			legacyFromL2Height, legacyToL2Height := fromL2Height, toL2Height
			if l2BedrockStartingHeight.Cmp(toL2Height) <= 0 {
				legacyToL2Height = new(big.Int).Sub(l2BedrockStartingHeight, bigint.One)
			}

			legacyBridgeLog := l2BridgeLog.New("mode", "legacy", "from_block_number", legacyFromL2Height, "to_block_number", legacyToL2Height)
			legacyBridgeLog.Info("scanning for legacy initiated bridge events")
			if err := bridge.LegacyL2ProcessInitiatedBridgeEvents(l2BridgeLog, tx, b.metrics, b.chainConfig.L2Contracts, legacyFromL2Height, legacyToL2Height); err != nil {
				return err
			} else if legacyToL2Height.Cmp(toL2Height) == 0 {
				return nil // a-ok! Entire range was legacy blocks
			}
			legacyBridgeLog.Info("detected switch to bedrock")
			fromL2Height = l2BedrockStartingHeight
		}

		l2BridgeLog = l2BridgeLog.New("from_block_number", fromL2Height, "to_block_number", toL2Height)
		l2BridgeLog.Info("scanning for initiated bridge events")
		return bridge.L2ProcessInitiatedBridgeEvents(l2BridgeLog, tx, b.metrics, b.chainConfig.L2Contracts, fromL2Height, toL2Height)
	}); err != nil {
		return err
	}

	b.LastL2Header = latestL2Header
	return nil
}

func (b *BridgeProcessor) processFinalizedL1Events(ctx context.Context) error {
	l1BridgeLog := b.log.New("bridge", "l1", "kind", "finalization")
	lastFinalizedL1BlockNumber := big.NewInt(int64(b.chainConfig.L1StartingHeight))
	if b.LastFinalizedL1Header != nil {
		lastFinalizedL1BlockNumber = b.LastFinalizedL1Header.Number
	}

	// Latest unfinalized L1 state bounded by `blockLimit` blocks that have had L2 bridge events indexed.
	latestL1HeaderScope := func(db *gorm.DB) *gorm.DB {
		return db.Where("number = (?)", db.Session(&gorm.Session{NewDB: true}).Model(database.L1BlockHeader{}).Select("MAX(number) as number").
			Order("number ASC").Limit(blocksLimit).Where("number > ? AND timestamp <= ?", lastFinalizedL1BlockNumber, b.LastL2Header.Timestamp))
	}

	latestL1Header, err := b.db.Blocks.L1BlockHeaderWithScope(latestL1HeaderScope)
	if err != nil {
		return fmt.Errorf("failed to query for latest unfinalized L1 state: %w", err)
	} else if latestL1Header == nil {
		return fmt.Errorf("no new L1 state found")
	}

	fromL1Height, toL1Height := new(big.Int).Add(lastFinalizedL1BlockNumber, bigint.One), latestL1Header.Number
	l1BridgeLog.Info("unobserved block range", "from_block_number", fromL1Height, "to_block_number", toL1Height)
	if err := b.db.Transaction(func(tx *database.DB) error {
		l1BedrockStartingHeight := big.NewInt(int64(b.chainConfig.L1BedrockStartingHeight))
		if l1BedrockStartingHeight.Cmp(fromL1Height) > 0 {
			legacyFromL1Height, legacyToL1Height := fromL1Height, toL1Height
			if l1BedrockStartingHeight.Cmp(toL1Height) <= 0 {
				legacyToL1Height = new(big.Int).Sub(l1BedrockStartingHeight, bigint.One)
			}

			legacyBridgeLog := l1BridgeLog.New("mode", "legacy", "from_block_number", legacyFromL1Height, "to_block_number", legacyToL1Height)
			legacyBridgeLog.Info("scanning for legacy finalized bridge events")
			if err := bridge.LegacyL1ProcessFinalizedBridgeEvents(l1BridgeLog, tx, b.metrics, b.l1Etl.EthClient, b.chainConfig.L1Contracts, legacyFromL1Height, legacyToL1Height); err != nil {
				return err
			} else if legacyToL1Height.Cmp(toL1Height) == 0 {
				return nil // a-ok! Entire range was legacy blocks
			}
			legacyBridgeLog.Info("detected switch to bedrock")
			fromL1Height = l1BedrockStartingHeight
		}

		l1BridgeLog = l1BridgeLog.New("from_block_number", fromL1Height, "to_block_number", toL1Height)
		l1BridgeLog.Info("scanning for finalized bridge events")
		return bridge.L1ProcessFinalizedBridgeEvents(l1BridgeLog, tx, b.metrics, b.chainConfig.L1Contracts, fromL1Height, toL1Height)
	}); err != nil {
		return err
	}

	b.LastFinalizedL1Header = latestL1Header
	return nil
}

func (b *BridgeProcessor) processFinalizedL2Events(ctx context.Context) error {
	l2BridgeLog := b.log.New("bridge", "l2", "kind", "finalization")
	lastFinalizedL2BlockNumber := bigint.Zero // skipping genesis
	if b.LastFinalizedL2Header != nil {
		lastFinalizedL2BlockNumber = b.LastFinalizedL2Header.Number
	}

	// Latest unfinalized L2 state bounded by `blockLimit` blocks that have had L1 bridge events indexed.
	latestL2HeaderScope := func(db *gorm.DB) *gorm.DB {
		return db.Where("number = (?)", db.Session(&gorm.Session{NewDB: true}).Model(database.L2BlockHeader{}).Select("MAX(number) as number").
			Order("number ASC").Limit(blocksLimit).Where("number > ? AND timestamp <= ?", lastFinalizedL2BlockNumber, b.LastL1Header.Timestamp))
	}

	latestL2Header, err := b.db.Blocks.L2BlockHeaderWithScope(latestL2HeaderScope)
	if err != nil {
		return fmt.Errorf("failed to query for latest unfinalized L2 state: %w", err)
	} else if latestL2Header == nil {
		return fmt.Errorf("no new L2 state found")
	}

	fromL2Height, toL2Height := new(big.Int).Add(lastFinalizedL2BlockNumber, bigint.One), latestL2Header.Number
	l2BridgeLog.Info("unobserved block range", "from_block_number", fromL2Height, "to_block_number", toL2Height)
	if err := b.db.Transaction(func(tx *database.DB) error {
		l2BedrockStartingHeight := big.NewInt(int64(b.chainConfig.L2BedrockStartingHeight))
		if l2BedrockStartingHeight.Cmp(fromL2Height) > 0 {
			legacyFromL2Height, legacyToL2Height := fromL2Height, toL2Height
			if l2BedrockStartingHeight.Cmp(toL2Height) <= 0 {
				legacyToL2Height = new(big.Int).Sub(l2BedrockStartingHeight, bigint.One)
			}

			legacyBridgeLog := l2BridgeLog.New("mode", "legacy", "from_block_number", legacyFromL2Height, "to_block_number", legacyToL2Height)
			l2BridgeLog.Info("scanning for legacy finalized bridge events")
			if err := bridge.LegacyL2ProcessFinalizedBridgeEvents(l2BridgeLog, tx, b.metrics, b.chainConfig.L2Contracts, legacyFromL2Height, legacyToL2Height); err != nil {
				return err
			} else if legacyToL2Height.Cmp(toL2Height) == 0 {
				return nil // a-ok! Entire range was legacy blocks
			}
			legacyBridgeLog.Info("detected switch to bedrock")
			fromL2Height = l2BedrockStartingHeight
		}

		l2BridgeLog = l2BridgeLog.New("from_block_number", fromL2Height, "to_block_number", toL2Height)
		l2BridgeLog.Info("scanning for finalized bridge events")
		return bridge.L2ProcessFinalizedBridgeEvents(l2BridgeLog, tx, b.metrics, b.chainConfig.L2Contracts, fromL2Height, toL2Height)
	}); err != nil {
		return err
	}

	b.LastFinalizedL2Header = latestL2Header
	return nil
}
