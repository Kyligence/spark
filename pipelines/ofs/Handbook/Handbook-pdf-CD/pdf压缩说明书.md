# Ghostscript

## PostScipt 语言和pdf解释器

### 1. 安装

``` bash
sudo apt-get install ghostscript
```

### 2. 使用

``` bash
gs -sDEVICE=pdfwrite -dCompatibilityLevel=1.4 -dPDFSETTINGS=/ebook -dNOPAUSE -dBATCH -dQUIET -sOutputFile=output.pdf input.pdf
```

### 3. 不同的压缩模式

``` bash
-dPDFSETTINGS=/screen,  #压缩比最大，输出文件最小，质量最低
-dPDFSETTINGS=/ebook,   #压缩比稍小，输出文件稍大，质量稍高
-dPDFSETTINGS=/prepress,#输出文件信息同Acrobat Distiller "Prepress Optimized"设置
-dPDFSETTINGS=/default, #缺省的，即大多数情况使用的压缩方式
gs -sDEVICE=pdfwrite -dCompatibilityLevel=1.4 -dDownsampleColorImages=true -dColorImageResolution=130 -dNOPAUSE -dBATCH -sOutputFile=output.pdf input.pdf

-dColorImageResolution=130 #可以设置图像DPI大小
```