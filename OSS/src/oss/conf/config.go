package conf

import (
	"bufio"
	"os"
	"oss/log"
	"io"
	"regexp"
	"strings"
	"strconv"
)

var configMap map[string]map[string]string

var commentRegexp *regexp.Regexp
var emptyRegexp *regexp.Regexp
var sectionRegexp *regexp.Regexp
var configRegexp *regexp.Regexp

/*
类ini配置解析
[user]
name = test
password = test
[system]
name = admin
password = admin
*/
func ConfigParser(filename string) {
	//"#"注释
	commentRegexp = regexp.MustCompile("^[[:space:]]*#.*")
	//空行
	emptyRegexp = regexp.MustCompile("^[[:space:]]*$")
	//章节
	sectionRegexp = regexp.MustCompile("^[[:space:]]*\\[[[:alnum:]]*\\]")
	//配置项
	configRegexp = regexp.MustCompile("^[[:space:]]*(?P<key>[[:word:]]+)[[:space:]]*=[[:space:]]*(?P<value>.*)")

	configMap = make( map[string]map[string]string)

	f, err := os.Open(filename)
	if err != nil {
		osslog.Fatalf("Open config file %v failed: %v.", filename, err)
	}
	defer f.Close()

	ret := processLine(f)
	if  ret != 0 {
		f.Close()
		osslog.Fatalf("Parse Configuration failed.")
	}
}

func GetSectionConf(section string) (map[string]string, bool) {
	conf, ok := configMap[section]
	return conf, ok
}

func GetString(section string, key string) (string, bool) {
	value, ok := configMap[section][key]
	return value, ok
}

func GetInt(section string, key string) (int, bool) {
	if value, ok := configMap[section][key]; ok {
		if rval, err := strconv.ParseInt(value, 10, 0); err != nil {
			return -1, false
		} else {
			return int(rval), true
		}
	} else {
		return -1, ok
	}
}

func GetInt64(section string, key string) (int64, bool) {
	if value, ok := configMap[section][key]; ok {
		if rval, err := strconv.ParseInt(value, 10, 64); err != nil {
			return -1, false
		} else {
			return rval, true
		}
	} else {
		return -1, ok
	}
}

func GetFloat32(section string, key string) (float32, bool) {
	if value, ok := configMap[section][key]; ok {
		if rval, err := strconv.ParseFloat(value, 32); err != nil {
			return -1, false
		} else {
			return float32(rval), true
		}
	} else {
		return -1, ok
	}
}

func GetFloat64(section string, key string) (float64, bool) {
	if value, ok := configMap[section][key]; ok {
		if rval, err := strconv.ParseFloat(value, 64); err != nil {
			return -1, false
		} else {
			return rval, true
		}
	} else {
		return -1, ok
	}
}

func processLine(f *os.File) int {
	var section string
	var data map[string]string
	buf := bufio.NewReader(f)

	for {
		line, err := buf.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			} else {
				osslog.Errorf("Read file content [%v] failed: %v", line, err)
				return -1
			}
		}

		//注释或空格跳过
		if commentRegexp.MatchString(line) || emptyRegexp.MatchString(line) {
			continue
		}

		//章节
		if sectionRegexp.MatchString(line) {
			section = trimSection(line)
			data = make(map[string]string)
			configMap[section] = data
			continue
		}

		//配置
		if configRegexp.MatchString(line) {
			k, v, err := parseKV(line)
			if err != 0 {
				osslog.Errorf("Invalid configuration: %v", line)
				return err
			}
			data[k] = v
		}
	}
	return 0
}

func parseKV(source string) (string, string, int) {
	temp := strings.Trim(source, "\n")
	trimed := strings.Trim(temp, " ")
	parts :=  strings.Split(trimed, "=")
	if len(parts) != 2 {
		return parts[0], parts[1], -1
	}
	return strings.Trim(parts[0], " "), strings.Trim(parts[1], " "), 0
}

func trimSection(source string) string {
	var start, end int
	for i, c := range source {
		if c == '[' {
			start = i
		}
		if c == ']' {
			end = i
			break
		}
	}
	return source[start + 1 : end]
}