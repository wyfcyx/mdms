num=$(( $1-1 ))
for i in $(seq 0 $num)
do
    #echo $i
    go run daemon.go $i &
done
