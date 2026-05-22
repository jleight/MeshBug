package store

import "time"

// ObserverUpsert is the data we extract from a /status message.
type ObserverUpsert struct {
	ID              []byte
	OriginName      string
	Region          string
	Model           string
	FirmwareVersion string
	ClientVersion   string
	Source          string
	RadioFreqKHz    *int
	RadioBWKHz      *float64
	RadioSF         *int
	RadioCR         *int
	Seen            time.Time
}

// StatusRow is one /status sample.
type StatusRow struct {
	ObserverID  []byte
	TS          time.Time
	Status      string
	UptimeSecs  *int64
	BatteryMV   *int
	QueueLen    *int
	NoiseFloor  *int
	TxAirSecs   *int64
	RxAirSecs   *int64
	RecvErrors  *int64
	LastRSSI    *int
	LastSNR     *float64
	DebugFlags  *int64
}

// PacketRow is one /packets observation, post-decode.
type PacketRow struct {
	TS             time.Time
	ObserverID     []byte
	PacketHash     []byte
	Direction      string
	PacketType     string
	Route          string
	Len            *int
	PayloadLen     *int
	RSSI           *int
	SNR            *float64
	Score          *int
	DurationMS     *int
	Raw            []byte
	DecodedType    *int16
	SourcePrefix   []byte
	DestPrefix     []byte
	TransportCodes []byte
}
