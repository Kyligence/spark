如果使用helm 部署服务，那么chart包管理就需要注意了，此示例意为部署完业务之后，将chart包自动推送到 harbor chart 仓库进行管理;
此示例是自己创建的测试 chart 目录，如果 helm 代码在 github 上，可以拉取相关代码，进入到相关目录，然后上传到harbor。 
此 harbor 仓库也要替换为自己的项目仓库，示例：https://harborurl/chartrepo/xxx  (xxx 为自定义的项目名称）
如果需要可将此事例的步骤整合到自己的pipeline中；

