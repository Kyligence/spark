#!/bin/bash -il
# 打包条件：
# 1. 需要 nvm 环境 或者 需要 node v10.24.1
# 2. 需要安装 gitbook:  npm install gitbook-cli -g
# 3. 需要组件：ebook-convert in Calibre

# Build book
#which node
#node -v
gitbook build

export PATH=$PATH:/opt/calibre
export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8

npm install svgexport -g
mv book.json book_tmp.json
sed -e '/\"star\"/d'  book_tmp.json > book.json
gitbook pdf
mv book_tmp.json book.json
echo "gitbook pdf success"
# There are chinese issue in ebook-convert, so we wget from gitbook https://www.gitbook.com/download/pdf/book/kyligence/kap_manual
#wget https://www.gitbook.com/download/pdf/book/kyligence/kap_manual?lang=zh-cn -O book_zh.pdf

mv book_en.pdf _book/en/Kyligence_Enterprise_4_5-en.pdf
rm -rf _book/zh-cn
mv _book/zh-hans _book/zh-cn
mv book_zh-hans.pdf _book/zh-cn/Kyligence_Enterprise_4_5-zh.pdf
#find _book -type f -name *.html |xargs -n 1 sed -i -E "s/\".+\/gitbook\/images\/favicon.ico\"/\"\/favicon.ico\"/g"
rm -rf _book/docker