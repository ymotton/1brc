sudo mkdir /tmp/ramdisk
sudo mount -t tmpfs -o size=20480M myramdisk /tmp/ramdisk/
sudo cp ./measurements.txt /tmp/ramdisk/
sudo dd if=/tmp/ramdisk/measurements.txt of=/dev/null bs=4k count=1000000
