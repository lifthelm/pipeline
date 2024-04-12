package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func runPipeline(taskAmount int) ([]LogOutInfo, time.Duration, time.Duration) {
	ProgramStartTime = time.Now()
	logInfo := make([]LogOutInfo, 0, DevicesAmount*TaskAmount*2)
	timeStart := time.Since(ProgramStartTime)
	for n := range deviceThreeFileDump(deviceTwoDictGenerator(deviceOneTermParser(taskGeneratorFromFiles(taskAmount)))) {
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
	return logInfo, timeStart, timeEnd
}

func runExp() {
	repeatAmount := 1
	taskAmounts := [...]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}

	for i, taskAmount := range taskAmounts {
		fmt.Println(i+1, "/", len(taskAmounts))
		outFileName := fmt.Sprintf("./raw_data/%d.txt", taskAmount)
		for j := 0; j < repeatAmount; j++ {
			file, err := os.Create(outFileName)
			if err != nil {
				log.Fatal(err)
			}

			logInfo, timeStart, timeEnd := runPipeline(taskAmount)

			writer := bufio.NewWriter(file)

			var sb strings.Builder
			sb.WriteString("System work time: ")
			sb.WriteString(strconv.FormatInt(int64(timeEnd-timeStart), 10))
			sb.WriteRune('\n')
			sb.WriteString("TaskAmount: ")
			sb.WriteString(strconv.FormatInt(int64(i+1), 10))
			sb.WriteRune('\n')

			_, err = writer.WriteString(sb.String())
			if err != nil {
				log.Fatalf("Got error while writing to a file. Err: %s", err.Error())
			}

			sort.Slice(logInfo, func(i, j int) bool {
				return logInfo[i].Time < logInfo[j].Time
			})
			for _, info := range logInfo {
				sb.Reset()
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
				sb.WriteRune('\n')
				_, err = writer.WriteString(sb.String())
				if err != nil {
					log.Fatalf("Got error while writing to a file. Err: %s", err.Error())
				}
			}
			writer.Flush()
			if err = file.Close(); err != nil {
				log.Fatal(err)
			}
		}
	}
}
