package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

var inFileName, outFileName, progName string

var bytesOut uint = 0
var bytesIn uint = 0

func readError() {
	fmt.Fprintf(os.Stderr,
		"%s: I/O error reading `%s', possible reason follows.\n",
		progName, inFileName)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "%s: warning: output file(s) may be incomplete.\n",
		progName)
	os.Exit(1)
}

func writeError() {
	fmt.Fprintf(os.Stderr,
		"%s: I/O error reading `%s', possible reason follows.\n",
		progName, inFileName)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "%s: warning: output file(s) may be incomplete.\n",
		progName)
	os.Exit(1)
}

type BitStream struct {
	handle   interface{}
	buffer   int32
	buffLive int32
	mode     string
	file     io.Closer
}

func openReadStream(stream *os.File) *BitStream {
	bs := &BitStream{bufio.NewReader(stream), 0, 0, "r", stream}
	return bs
}

func openWriteStream(stream *os.File) *BitStream {
	bs := &BitStream{bufio.NewWriter(stream), 0, 0, "w", stream}
	return bs
}

func putBit(bs *BitStream, bit int32) {
	if bs.buffLive == 8 {
		_, err := bs.handle.(io.Writer).Write([]byte{byte(bs.buffer)})
		if err != nil {
			writeError()
		}
		bytesOut++
		bs.buffLive = 1
		bs.buffer = bit & 0x1
	} else {
		bs.buffer = ((bs.buffer << 1) | (bit & 0x1))
		bs.buffLive++
	}
}

func getBit(bs *BitStream) int32 {
	if bs.buffLive > 0 {
		bs.buffLive--
		return int32((uint(bs.buffer) >> uint(bs.buffLive)) & 0x1)
	} else {
		retVal := make([]byte, 1)
		_, err := bs.handle.(io.Reader).Read(retVal)
		if err == io.EOF {
			return 2
		}
		if err != nil {
			readError()
		}
		bs.buffLive = 7
		bs.buffer = int32(retVal[0])
		return ((bs.buffer) >> 7) & 0x1
	}
}

func close(bs *BitStream) {

	if bs.mode == "w" {
		for bs.buffLive < 8 {
			bs.buffLive++
			bs.buffer <<= 1
		}
		_, err := bs.handle.(io.Writer).Write([]byte{byte(bs.buffer)})
		if err != nil {
			writeError()
		}
		bytesOut++
		err = bs.handle.(*bufio.Writer).Flush()
		if err != nil {
			writeError()
		}
	}
	err := bs.file.Close()
	if err != nil {
		if bs.mode == "w" {
			writeError()
		} else {
			readError()
		}
	}
}

func putByte(bs *BitStream, c byte) {
	for i := 7; i >= 0; i-- {
		putBit(bs, int32((uint(c)>>uint(i))&0x1))
	}
}

func putUint32(bs *BitStream, c uint32) {
	for i := 31; i >= 0; i-- {
		putBit(bs, int32((uint(c)>>uint(i))&0x1))
	}
}

func endsInBz2(name string) bool {
	n := len(name)
	if n <= 4 {
		return false
	} else {
		return (name[n-4] == '.' &&
			name[n-3] == 'b' &&
			name[n-2] == 'z' &&
			name[n-1] == '2')
	}
}

const (
	BLOCK_HEADER_HI = 0x00003141
	BLOCK_HEADER_LO = 0x59265359

	BLOCK_ENDMARK_HI = 0x00001772
	BLOCK_ENDMARK_LO = 0x45385090
)

const BUFFER_SIZE = 20000

var bStart [BUFFER_SIZE]uint32
var bEnd [BUFFER_SIZE]uint32
var rbStart [BUFFER_SIZE]uint32
var rbEnd [BUFFER_SIZE]uint32

func main() {
	var inFile *os.File
	var outFile *os.File
	var bsIn *BitStream
	var bsWr *BitStream
	var currBlock, b, wrBlock int32
	var bitsRead uint32
	var rbCtr int32

	var buffHi, buffLo, blockCRC uint32

	progName = os.Args[0]
	inFileName = ""
	outFileName = ""

	fmt.Fprintf(os.Stderr, "bzip2recover: extracts blocks from damaged .bz2 files.\n")

	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "%s: usage is `%s damaged_file_name'.\n",
			progName, progName)
		os.Exit(1)
	}

	inFileName = os.Args[1]

	inFile, err := os.Open(inFileName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: can't read `%s'\n", progName, inFileName)
		os.Exit(1)
	}

	bsIn = openReadStream(inFile)
	fmt.Fprintf(os.Stderr, "%s: searching for block boundaries...\n", progName)

	bitsRead = 0
	buffHi = 0
	buffLo = 0
	currBlock = 0
	bStart[currBlock] = 0

	rbCtr = 0

	for {
		b = getBit(bsIn)
		bitsRead++
		if b == 2 {
			if bitsRead >= bStart[currBlock] &&
				(bitsRead-bStart[currBlock]) >= 40 {
				bEnd[currBlock] = bitsRead - 1
				if currBlock > 0 {
					fmt.Fprintf(os.Stderr, "   block %d runs from %d to %d (incomplete)\n",
						currBlock, bStart[currBlock], bEnd[currBlock])
				}
			} else {
				currBlock--
			}
			break
		}

		buffHi = uint32((uint(buffHi) << 1) | (uint(buffLo) >> 31))
		buffLo = uint32((uint(buffLo) << 1) | (uint(b) & 1))

		if ((buffHi&0x0000ffff) == BLOCK_HEADER_HI && buffLo == BLOCK_HEADER_LO) ||
			((buffHi&0x0000ffff) == BLOCK_ENDMARK_HI && buffLo == BLOCK_ENDMARK_LO) {
			if bitsRead > 49 {
				bEnd[currBlock] = bitsRead - 49
			} else {
				bEnd[currBlock] = 0
			}
			if currBlock > 0 && (bEnd[currBlock]-bStart[currBlock]) >= 130 {
				fmt.Fprintf(os.Stderr, "   block %d runs from %d to %d\n",
					rbCtr+1, bStart[currBlock], bEnd[currBlock])
				rbStart[rbCtr] = bStart[currBlock]
				rbEnd[rbCtr] = bEnd[currBlock]
				rbCtr++
			}
			currBlock++

			bStart[currBlock] = bitsRead
		}
	}

	close(bsIn)

	if rbCtr < 1 {
		fmt.Fprintf(os.Stderr, "%s: sorry, I couldn't find any block boundaries.\n",
			progName)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "%s: splitting into blocks\n", progName)

	inFile, err = os.Open(inFileName)
	if inFile == nil {
		fmt.Fprintf(os.Stderr, "%s: can't open `%s'\n", progName, inFileName)
		os.Exit(1)
	}
	bsIn = openReadStream(inFile)

	blockCRC = 0
	bsWr = nil

	bitsRead = 0
	outFile = nil
	wrBlock = 0

	for {
		b = getBit(bsIn)
		if b == 2 {
			break
		}

		buffHi = (buffHi << 1) | (buffLo >> 31)
		buffLo = (buffLo << 1) | (uint32(b) & 1)

		if bitsRead == 47+rbStart[wrBlock] {
			blockCRC = (buffHi << 16) | (buffLo >> 16)
		}

		if outFile != nil &&
			bitsRead >= rbStart[wrBlock] &&
			bitsRead <= rbEnd[wrBlock] {
			putBit(bsWr, b)
		}

		bitsRead++

		if bitsRead == (rbEnd[wrBlock] + 1) {
			if outFile != nil {
				putByte(bsWr, 0x17)
				putByte(bsWr, 0x72)
				putByte(bsWr, 0x45)
				putByte(bsWr, 0x38)
				putByte(bsWr, 0x50)
				putByte(bsWr, 0x90)
				putUint32(bsWr, blockCRC)
				close(bsWr)
			}

			if wrBlock >= rbCtr {
				break
			}

			wrBlock++

		} else if bitsRead == rbStart[wrBlock] {

			outFileName = fmt.Sprintf("rec%4d", wrBlock+1)
			outFileName = strings.Replace(outFileName, " ", "0", -1)
			outFileName += inFileName
			if !endsInBz2(outFileName) {
				outFileName += ".bz2"
			}

			fmt.Fprintf(os.Stderr, "   writing block %d to `%s' ...\n", wrBlock+1, outFileName)

			outFile, err = os.Create("rec/" + outFileName)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: can't write `%s'\n", progName, outFileName)
				os.Exit(1)
			}
			bsWr = openWriteStream(outFile)
			putByte(bsWr, 'B')
			putByte(bsWr, 'Z')
			putByte(bsWr, 'h')
			putByte(bsWr, '9')
			putByte(bsWr, 0x31)
			putByte(bsWr, 0x41)
			putByte(bsWr, 0x59)
			putByte(bsWr, 0x26)
			putByte(bsWr, 0x53)
			putByte(bsWr, 0x59)
		}
	}

	fmt.Fprintf(os.Stderr, "%s: finished\n", progName)
	os.Exit(0)
}
