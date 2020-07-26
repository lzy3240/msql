package msql

import (
	"errors"
	"strconv"
	"time"
)

//DecideType ..
func DecideType(src interface{}) (string, error) {
	tmp := ""
	switch t := src.(type) {
	case nil:
		tmp = "null"
	case bool:
		if t {
			tmp = "True"
		} else {
			tmp = "False"
		}
	case []byte:
		tmp = string(t)
	case time.Time:
		tmp = t.Format("2006-01-02 15:04:05.999")
	case int:
		tmp = strconv.Itoa(t)
	case int32:
		tmp = strconv.Itoa(int(t))
	case int64:
		tmp = strconv.FormatInt(t, 10)
	case string:
		tmp = string(t)
	default:
		//log.Error("no" this type %T", t) //return errors.New("no this type")
		err := errors.New("no this type")
		return tmp, err
	}
	return tmp, nil
}
