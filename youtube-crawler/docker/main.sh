#!/bin/bash

function finalize_err() {
    CODE=$?
    echo $TEXT_ERR
   
    trap - ERR

    exit $CODE
}

trap finalize_err ERR

for var in INPUT_DIR INPUT_FILE OUTPUT_DIR LOGS_DIR KAFKA_SERVER YT_CHANNELS_TOPIC YT_VIDEOS_TOPIC API_KEY; do
    TEXT_ERR="$var variable is not set."
    [ ! -z ${!var} ]
done

for d in INPUT_DIR OUTPUT_DIR LOGS_DIR; do #datasets created if missing
    TEXT_ERR="$d=${!d} directory does not exist."
    [ -d ${!d} ]
done

for f in INPUT_FILE; do
    valf=${!f}
    if [[ $valf == /* ]]; then
        TEXT_ERR="$f=$valf file does not exist."
        [ -f $valf ]
    else
        TEXT_ERR="$f=$INPUT_DIR/$valf file does not exist."
        [ -f $INPUT_DIR"/"$valf ]
        eval $f=$INPUT_DIR/$valf
    fi
done

COMMAND="python YoutubeChannelCrawler.py -i $INPUT_FILE -o $OUTPUT_DIR -b $KAFKA_SERVER -c $YT_CHANNELS_TOPIC -v $YT_VIDEOS_TOPIC -k $API_KEY"
echo $COMMAND 

cd /youtube-crawler/ && $COMMAND 2>&1 | tee -a $LOGS_DIR/syslog.txt