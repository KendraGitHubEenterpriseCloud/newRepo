
set -ex

for i in {2..3}
do
    echo  "Adding new line $i">> test_file.txt
    git add .
    git commit -m "Adding new line using shell script $i"
    git push
done


echo  >> test_file.txt
