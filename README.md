# Inserting 1 Million data into BigQuery Table

## Introduction

This is the source code of my blog post "Inserting 1 Million data into BigQuery Table".
In this blog post, we will demonstrate how to insert JSON text file into Google's BigQuery Table using Go language. Go is known for one of the best language to write high-performance programs due to it's native libraries that make concurrent and parallel programming easier. We will also demonstrate how to use Go various native libraries (channel, goroutine, waitgroup).

## Usage

### Installation

```sh
# Install Go (1.12.1)
go version
# go version go1.12.1 linux/amd64

# Clone this project
go get https://github.com/anpandu/go-json-to-bq

# Install dependencies
cd $GOPATH/src/github.com/anpandu/go-json-to-bq
go get

# Generate dataset
python3 gen-txt.py
# you will see files like this
# -rw-rw-r-- 1 pandu pandu  220 Mar  5 18:31 students-10.json.txt
# -rw-rw-r-- 1 pandu pandu 2.3K Mar  5 18:31 students-100.json.txt
# -rw-rw-r-- 1 pandu pandu  24K Mar  5 18:31 students-1000.json.txt
# -rw-rw-r-- 1 pandu pandu 244K Mar  5 18:31 students-10000.json.txt
# -rw-rw-r-- 1 pandu pandu 2.5M Mar  5 18:31 students-100000.json.txt
# -rw-rw-r-- 1 pandu pandu  26M Mar  5 18:31 students-1000000.json.txt

# Enable credential
export GOOGLE_APPLICATION_CREDENTIALS=~/mykey.json
```

### How to use

```sh
# Run Part One: Simple Approach
go run cmd/main1/main1.go \
  --project=myproject \
  --dataset=mydataset \
  --table=mytable \
  --filepath=./students-10.json.txt

# Run Part Two: Multiple Rows Insertion
go run cmd/main2/main2.go \
  --project=myproject \
  --dataset=mydataset \
  --table=mytable \
  --buffer-length=100 \
  --filepath=./students-10.json.txt

# Part Three: Multiple Workers and Multiple Rows Insertion
go run cmd/main3/main3.go \
  --project=myproject \
  --dataset=mydataset \
  --table=mytable \
  --buffer-length=100 \
  --worker=4 \
  --filepath=./students-10.json.txt
```

## Benchmark

This benchmark is taken using same type of machine, ___. Using multiple JSON text files generated at different sizes, we benchmark three approaches and measure time taken.

| File       | Parameter   | 10        | 1.000     | 1.000.000 |
|:-----------|:------------|----------:|----------:|----------:|
| main.go    | w1 - n1     | 312.1648s | 312.1648s | 5.3089s   |
| main2.go   | w1 - n100   |   4.5991s |   4.5991s | 2.6385s   |
| main3.go   | w4 - n100   |   1.4532s |   1.4532s | 2.0018s   |
| main3.go   | w16 - n100  |   0.8080s |   0.8080s | 2.0018s   |
| main3.go   | w64 - n100  |   0.8487s |   0.8487s | 2.0018s   |
| main3.go   | w300 - n500 |   0.9348s |   0.9348s | 2.0018s   |

As we can see, higher number of `w` and `n` make insertion faster. Using our highest configuration enable us to insert one million rows in a mere 14 seconds!

## License

MIT © [Ananta Pandu](anpandumail@gmail.com)