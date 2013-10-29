#!/bin/bash

HADOOP_VERSION=1.2.1
HADOOP_HOME=/home/alstein/bin/hadoop-${HADOOP_VERSION}/
PATH=${HADOOP_HOME}/bin:$PATH

function hall() {
    set -x

    local src=$1
    local package=$2
    local class=$3
    local classes_directory="classes"

    if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ] ; then
        echo "Usage hall src package class (like Source.java package.jar com.example.Main)"
        return
    fi

    rm -rf $classes_directory temp
    mkdir -p $classes_directory

    hcompile $classes_directory $src
    hpackage $package $classes_directory
    hrun $package $class "${@:4}"

    hocat '*'

    set +x
}

function hcompile() {
    local package_directory=$1
    local src=$2
    if [ -z "$package_directory" ] || [ -z "$src" ]; then
    echo "Usage hcompile package_directory source.java"
        return
    fi
    javac -classpath ${HADOOP_HOME}/hadoop-core-${HADOOP_VERSION}.jar:'../lib/' -d $package_directory $src
}

function hpackage() {
    local package=$1
    local package_directory=$2
    if [ -z "$package" ] || [ -z "$package_directory" ] ; then
        echo "Usage hpackage package package_directory"
        return
    fi
    jar -cvf $package -C $package_directory .
}

function hls() {
    hadoop dfs -ls input
}

function hicat() {
    hadoop dfs -cat input/$1
}

function hocat() {
    hadoop dfs -cat output/$1
}

function hrun() {
    local package=$1
    local class=$2
    if [ -z "$package" ] || [ -z "$class" ] ; then
        echo "Usage hrun package class"
        return
    fi
    rm -rf output
    hadoop jar $package $class input output "${@:3}"
}


