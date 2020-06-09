num=$(( $1-1 ))
for i in $(seq 0 $num)
do
    #echo $i
    daemon $i 1 &
done
