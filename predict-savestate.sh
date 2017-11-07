#! /bin/bash

check()
{
    if (test -z "$1") ; then
        echo "predict.sh <WORKING_DRIECTORY> <REGION> <AZ>"
        exit 1
    fi
}

BIN=./bin/
HERE=$1
REGION=$2
AZ=$3

check "$HERE"
check "$REGION"
check "$AZ"

sort $HERE/data.txt > $HERE/sorted.txt
uniq $HERE/sorted.txt > $HERE/uniqed.txt

FILE="$HERE/uniqed.txt"

mkdir -p "$HERE/$AZ"

for TYPE in `awk '{print $3}' $FILE | uniq` ; do
		grep $AZ $FILE | grep $TYPE | grep Linux | grep -v SUSE > $HERE/$AZ/$TYPE.data
done

MAXSIZE=100000
PERIOD=$((2592000*3))

INSTTYPE="Linux"

TARG=$HERE/results
STATE=$HERE/bmbp_state

# GMTOFF=`date --rfc-3339="seconds" | awk -F '-' '{print $4}' | awk -F':' '{print $1}'`

mkdir -p $TARG

GMTOFF=`date +%z`
NOW=`/bin/date +%s`
NOW=$(($NOW-($GMTOFF*3600)))
# 3 month earlier
EARLIEST=$(($NOW-$PERIOD))

for TYPE in `awk '{print $3}' $FILE | sort | uniq` ; do
    echo $TYPE

    grep $INSTTYPE $HERE/$AZ/$TYPE.data | grep -v SUSE | sort -k 6 | awk '{print $6,$5}' | sed 's/T/ /' | sed 's/Z//' > $HERE/$AZ/$TYPE.$INSTTYPE.temp1

    STEST=`wc -l $HERE/$AZ/$TYPE.$INSTTYPE.temp1 | awk '{print $1}'`

    if ( test $STEST -le 0 ) ; then
        continue
    fi

    $BIN/convert_time -f $HERE/$AZ/$TYPE.$INSTTYPE.temp1 | sort -n -k 1 -k 2 | awk -v gmt=$GMTOFF '{print $1-(gmt*3600),$2}' | awk -v early=$EARLIEST '{if($1 >= early) {print $1,$2}}' > $HERE/$AZ/$TYPE.$INSTTYPE.temp2

    F="$HERE/$AZ/$TYPE.$INSTTYPE.temp2"

    $BIN/spot-price-aggregate -f $F | sort -n -k 1 -k 2 | uniq | awk '{if ($1 > 1422228412) {print $1,$2}}' > $TARG/$REGION-$AZ-$TYPE-agg.txt

    if [ -e $STATE/$TYPE.state ] 
    then 
        $BIN/bmbp_ts -f $TARG/$REGION-$AZ-$TYPE-agg.txt -T -i 350 -q 0.975 -c 0.01 --loadstate $STATE/$TYPE.loadstate --savestate $STATE/$TYPE.savestate | grep "pred:" | awk '{print $2,$4,($6+0.0001),$14}' > $TARG/$REGION-$AZ-$TYPE-pred.txt
    else 
        $BIN/bmbp_ts -f $TARG/$REGION-$AZ-$TYPE-agg.txt -T -i 350 -q 0.975 -c 0.01 --savestate $STATE/$TYPE.savestate | grep "pred:" | awk '{print $2,$4,($6+0.0001),$14}' > $TARG/$REGION-$AZ-$TYPE-pred.txt
    fi

    awk '{print $1,$2,$3}' $TARG/$REGION-$AZ-$TYPE-pred.txt > $TARG/$REGION-$AZ-$TYPE-temp.txt

    $BIN/pred-duration -f $TARG/$REGION-$AZ-$TYPE-temp.txt -T 0 -e | sort -n -k 2 > $TARG/$REGION-$AZ-$TYPE-duration.txt
    
    $BIN/pred-distribution-fast -f $TARG/$REGION-$AZ-$TYPE-temp.txt -q 0.025 -c 0.99 -F 4.0 -I 0.05 | awk '{print $1/3600,$2}' > $TARG/$TYPE.pgraph

    DSIZE=`wc -l $TARG/$REGION-$AZ-$TYPE-duration.txt | awk '{print $1}'`

    $BIN/bmbp_index -s $DSIZE -q 0.025 -c 0.99 > $TARG/$REGION-$AZ-$TYPE-ndx.txt
    
done
