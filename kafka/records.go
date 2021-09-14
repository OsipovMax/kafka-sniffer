package kafka

import "fmt"

const (
	unknownRecords = iota
	legacyRecords
	defaultRecords

	magicOffset = 16
)

// Records implements a union type containing either a RecordBatch or a legacy MessageSet.
type Records struct {
	recordsType int
	MsgSet      *MessageSet
	RecordBatch *RecordBatch
}

func newLegacyRecords(msgSet *MessageSet) Records {
	return Records{recordsType: legacyRecords, MsgSet: msgSet}
}

func newDefaultRecords(batch *RecordBatch) Records {
	return Records{recordsType: defaultRecords, RecordBatch: batch}
}

	func (r *Records) setTypeFromMagic(pd PacketDecoder) error {
	magic, err := magicValue(pd)
	if err != nil {
		return err
	}

	r.recordsType = defaultRecords
	if magic < 2 {
		r.recordsType = legacyRecords
	}

	return nil
}

func magicValue(pd PacketDecoder) (int8, error) {
	return pd.peekInt8(magicOffset)
}

func (r *Records) decode(pd PacketDecoder) error {
	if r.recordsType == unknownRecords {
		if err := r.setTypeFromMagic(pd); err != nil {
			return err
		}
	}

	switch r.recordsType {
	case legacyRecords:
		r.MsgSet = &MessageSet{}
		return r.MsgSet.Decode(pd)
	case defaultRecords:
		r.RecordBatch = &RecordBatch{}
		return r.RecordBatch.decode(pd)
	}
	return fmt.Errorf("unknown records type: %v", r.recordsType)
}

func (r *Records) setTypeFromFields() (bool, error) {
	if r.MsgSet == nil && r.RecordBatch == nil {
		return true, nil
	}
	if r.MsgSet != nil && r.RecordBatch != nil {
		return false, fmt.Errorf("both MsgSet and RecordBatch are set, but record type is unknown")
	}
	r.recordsType = defaultRecords
	if r.MsgSet != nil {
		r.recordsType = legacyRecords
	}
	return false, nil
}

func (r *Records) isOverflow() (bool, error) {
	if r.recordsType == unknownRecords {
		if empty, err := r.setTypeFromFields(); err != nil || empty {
			return false, err
		}
	}

	switch r.recordsType {
	case unknownRecords:
		return false, nil
	case legacyRecords:
		if r.MsgSet == nil {
			return false, nil
		}
		return r.MsgSet.OverflowMessage, nil
	case defaultRecords:
		return false, nil
	}
	return false, fmt.Errorf("unknown records type: %v", r.recordsType)
}

func (r *Records) numRecords() (int, error) {
	if r.recordsType == unknownRecords {
		if empty, err := r.setTypeFromFields(); err != nil || empty {
			return 0, err
		}
	}

	switch r.recordsType {
	case legacyRecords:
		if r.MsgSet == nil {
			return 0, nil
		}
		return len(r.MsgSet.Messages), nil
	case defaultRecords:
		if r.RecordBatch == nil {
			return 0, nil
		}
		return len(r.RecordBatch.Records), nil
	}
	return 0, fmt.Errorf("unknown records type: %v", r.recordsType)
}

func (r *Records) isPartial() (bool, error) {
	if r.recordsType == unknownRecords {
		if empty, err := r.setTypeFromFields(); err != nil || empty {
			return false, err
		}
	}

	switch r.recordsType {
	case unknownRecords:
		return false, nil
	case legacyRecords:
		if r.MsgSet == nil {
			return false, nil
		}
		return r.MsgSet.PartialTrailingMessage, nil
	case defaultRecords:
		if r.RecordBatch == nil {
			return false, nil
		}
		return r.RecordBatch.PartialTrailingRecord, nil
	}
	return false, fmt.Errorf("unknown records type: %v", r.recordsType)
}