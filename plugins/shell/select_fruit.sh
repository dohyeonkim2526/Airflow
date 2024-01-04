echo "Please select fruits between (APPLE or ORANGE or GRAPE)."
echo "You need to enter in English capital letters."

FRUIT=$1

if [ $FRUIT == APPLE ]; then
    echo "You selected Apple!"
elif [ $FRUIT == ORANGE ]; then
    echo "You selected Orange!"
elif [ $FRUIT == GRAPE ]; then
    echo "You selected Grape!"
else
    echo "You selected other Fruit!"
fi
