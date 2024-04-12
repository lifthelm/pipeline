package main

/*
5 лаба

конвейер
текст входной -> 1 <-> pymorphy2
-> список словоформ
2 -> словарь(если слишком долго, то запустить параллельную версию)
3 -> дамп словаря в новый файл

если 1 и 2я работают примерно одинаково оставить последовательную версию
*/

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ProgramStartTime time.Time

type DeviceID uint8

const DeviceBufferSize = 10
const DevicesAmount = 4

const (
	DeviceGenerateID = iota
	DeviceOneID
	DeviceTwoID
	DeviceThreeID
)

type TimeDeviceInfo struct {
	DID   DeviceID
	Start time.Duration
	End   time.Duration
}

type LogInfo struct {
	TaskID          uint32
	DevicesTimeInfo []TimeDeviceInfo
}

const (
	DeviceStart = iota
	DeviceEnd
)

type LogOutInfo struct {
	TaskID  uint32
	DID     DeviceID
	Time    time.Duration
	LogType uint8
}

type Task struct {
	Words    []string
	Terms    []Term
	TermDict map[Term]uint8
	LogInfo  LogInfo
}

func taskGeneratorFromFile(filename string, i int, out chan<- Task) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanWords)
	words := make([]string, 0, 64)
	for scanner.Scan() {
		words = append(words, scanner.Text())
		//fmt.Println(scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	newTask := Task{Words: words, Terms: make([]Term, 0, len(words)), LogInfo: LogInfo{DevicesTimeInfo: make([]TimeDeviceInfo, 0, DevicesAmount), TaskID: uint32(i)}}
	newTask.LogInfo.DevicesTimeInfo = append(newTask.LogInfo.DevicesTimeInfo, TimeDeviceInfo{DID: DeviceGenerateID, Start: time.Since(ProgramStartTime)})
	out <- newTask
}

const DefaultTaskLimit = 3

func getTaskFileNames(taskLimit int) []string {
	entries, err := os.ReadDir("./data/")
	if err != nil {
		log.Fatal("Cant open requests data dir")
	}
	res := make([]string, 0, len(entries))
	for i, v := range entries {
		if i >= taskLimit {
			break
		}
		var sb strings.Builder
		sb.WriteString("./data/")
		sb.WriteString(v.Name())
		res = append(res, sb.String())
	}
	return res
}

func taskGeneratorFromFiles(taskAmount int) <-chan Task {
	// TODO list all task files in ./data dir
	out := make(chan Task, DeviceBufferSize)
	go func() {
		for i, filename := range getTaskFileNames(taskAmount) {
			//fmt.Println(filename)
			//continue
			taskGeneratorFromFile(filename, i, out)
		}
		//fmt.Println("Closing generator")
		close(out)
	}()
	return out
}

type TermTask struct {
	Word string
	Term Term
	err  error
}

func subDeviceTermParer(in <-chan TermTask) <-chan TermTask {
	out := make(chan TermTask, 1)
	go func() {
		for termTask := range in {
			term, err := ParseWord(termTask.Word)
			if err != nil {
				termTask.err = err
			}
			termTask.Term = term

			out <- termTask
		}
		close(out)
	}()
	return out
}

const SubDeviceAmount = 8

func subDevicesTermParsers(in <-chan TermTask) <-chan TermTask {
	out := make(chan TermTask, SubDeviceAmount)
	wg := sync.WaitGroup{}
	mergeOutput := func() {
		subOut := subDeviceTermParer(in)
		for n := range subOut {
			out <- n
		}
		wg.Done()
	}
	wg.Add(SubDeviceAmount)
	for i := 0; i < SubDeviceAmount; i++ {
		go mergeOutput()
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func deviceOneTermParser(in <-chan Task) <-chan Task {
	out := make(chan Task, DeviceBufferSize)

	subIn := make(chan TermTask, SubDeviceAmount)
	subOut := subDevicesTermParsers(subIn)
	go func() {
		for task := range in {
			timeStart := time.Since(ProgramStartTime)
			go func() {
				for _, word := range task.Words {
					subIn <- TermTask{Word: word}
				}
			}()
			for i := 0; i < len(task.Words); i++ {
				termRes := <-subOut
				if termRes.err != nil {
					continue
				}
				task.Terms = append(task.Terms, termRes.Term)
				//fmt.Printf("%d/%d %v\n", i, len(task.Words), termRes.Term)
			}
			timeEnd := time.Since(ProgramStartTime)
			task.LogInfo.DevicesTimeInfo = append(task.LogInfo.DevicesTimeInfo, TimeDeviceInfo{
				DID:   DeviceOneID,
				Start: timeStart,
				End:   timeEnd,
			})
			out <- task
		}
		//fmt.Println("Closing device one")
		close(subIn)
		close(out)
	}()
	return out
}

func deviceTwoDictGenerator(in <-chan Task) <-chan Task {
	out := make(chan Task, DeviceBufferSize)

	go func() {
		for task := range in {
			timeStart := time.Since(ProgramStartTime)
			task.TermDict = make(map[Term]uint8)
			for _, term := range task.Terms {
				k, ok := task.TermDict[term]
				if ok {
					task.TermDict[term] = k + 1
				} else {
					task.TermDict[term] = 1
				}
			}
			timeEnd := time.Since(ProgramStartTime)
			task.LogInfo.DevicesTimeInfo = append(task.LogInfo.DevicesTimeInfo, TimeDeviceInfo{
				DID:   DeviceTwoID,
				Start: timeStart,
				End:   timeEnd,
			})
			out <- task
		}
		//fmt.Println("Closing device two")
		close(out)
	}()
	return out
}

func deviceThreeFileDump(in <-chan Task) <-chan Task {
	out := make(chan Task, DeviceBufferSize)

	go func() {
		for task := range in {
			timeStart := time.Since(ProgramStartTime)

			file, err := os.Create("./data_out/out.txt")
			if err != nil {
				log.Fatal(err)
			}

			writer := bufio.NewWriter(file)
			for term, freq := range task.TermDict {
				var sb strings.Builder
				sb.WriteString(term.NormalForm)
				sb.WriteRune(' ')
				sb.WriteString(strconv.FormatInt(int64(freq), 10))
				_, err = writer.WriteString(sb.String() + "\n")
				if err != nil {
					log.Fatalf("Got error while writing to a file. Err: %s", err.Error())
				}
			}
			writer.Flush()
			if err = file.Close(); err != nil {
				log.Fatal(err)
			}

			timeEnd := time.Since(ProgramStartTime)
			task.LogInfo.DevicesTimeInfo = append(task.LogInfo.DevicesTimeInfo, TimeDeviceInfo{
				DID:   DeviceThreeID,
				Start: timeStart,
				End:   timeEnd,
			})
			out <- task
		}
		//fmt.Println("Closing device three")
		close(out)
	}()
	return out
}

const TaskAmount = 5

func main() {
	ProgramStartTime = time.Now()
	logInfo := make([]LogOutInfo, 0, DevicesAmount*TaskAmount*2)
	timeStart := time.Since(ProgramStartTime)
	for n := range deviceThreeFileDump(deviceTwoDictGenerator(deviceOneTermParser(taskGeneratorFromFiles(DefaultTaskLimit)))) {
		n.LogInfo.DevicesTimeInfo[0].End = time.Since(ProgramStartTime)
		for _, info := range n.LogInfo.DevicesTimeInfo {
			logInfo = append(logInfo, LogOutInfo{
				TaskID:  n.LogInfo.TaskID,
				DID:     info.DID,
				Time:    info.Start,
				LogType: DeviceStart,
			}, LogOutInfo{
				TaskID:  n.LogInfo.TaskID,
				DID:     info.DID,
				Time:    info.End,
				LogType: DeviceEnd,
			})
		}
	}
	timeEnd := time.Since(ProgramStartTime)
	sort.Slice(logInfo, func(i, j int) bool {
		return logInfo[i].Time < logInfo[j].Time
	})
	fmt.Println("System work time: ", (timeEnd - timeStart).Nanoseconds())
	fmt.Println("Tasks amount: ", len(logInfo)/2)
	for _, info := range logInfo {
		var sb strings.Builder
		sb.WriteString("TID: ")
		sb.WriteString(strconv.FormatInt(int64(info.TaskID+1), 10))
		sb.WriteString(" DID: ")
		sb.WriteString(strconv.FormatInt(int64(info.DID), 10))
		if info.LogType == DeviceStart {
			sb.WriteString(" start")
		} else {
			sb.WriteString(" end")
		}
		sb.WriteRune('\t')
		sb.WriteString(strconv.FormatInt(info.Time.Nanoseconds(), 10))
		fmt.Println(sb.String())
	}
}
