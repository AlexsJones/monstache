package types

import "fmt"

type Stringargs []string

func (args *Stringargs) String() string {
	return fmt.Sprintf("%s", *args)
}

func (args *Stringargs) Set(value string) error {
	*args = append(*args, value)
	return nil
}
