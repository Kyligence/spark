#!/bin/bash -il
# echo "gitbook install plugins"
# gitbook install

# echo "check deadlines"

echo "start generation pdf ..."
gitbook pdf
echo "pdf generated"

echo "....setup pdf...."
mv book_en.pdf _book/en/Kyligence_Enterprise_4_5-en.pdf
rm -rf _book/zh-cn
mv _book/zh-hans _book/zh-cn
mv book_zh-hans.pdf _book/zh-cn/Kyligence_Enterprise_4_5-zh.pdf

echo "cleanup..."
rm -rf _book/docker

echo "compressing..."
cd _book/
tar -zcvf /book/enterprise_v4.5.tar.gz * > /dev/null

echo "uploading..."
