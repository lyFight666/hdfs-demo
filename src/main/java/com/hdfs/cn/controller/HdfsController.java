package com.hdfs.cn.controller;

/**
 * @Author: Mr.L
 * @Date: 2020/10/15 11:05
 **/
import java.util.List;
import java.util.Map;

import com.hdfs.cn.pojo.entity.User;
import com.hdfs.cn.pojo.vo.Result;
import com.hdfs.cn.service.HdfsService;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.BlockLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;


@RestController
@RequestMapping("/hadoop/hdfs")
public class HdfsController {

    private static Logger LOGGER = LoggerFactory.getLogger(HdfsController.class);
    @Resource
    HdfsService hdfsService;
    /**
     * 创建文件夹（测试完成）
     * @param path
     * @return
     * @throws Exception
     */
    @PostMapping("mkdir")
    public Result mkdir(@RequestParam("path") String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            LOGGER.debug("请求参数为空");
            return new Result("FAILURE", "请求参数为空");
        }
        // 创建空文件夹
        boolean isOk = hdfsService.mkdir(path);
        if (isOk) {
            LOGGER.debug("文件夹创建成功");
            return new Result("SUCCESS", "文件夹创建成功");
        } else {
            LOGGER.debug("文件夹创建失败");
            return new Result("FAILURE", "文件夹创建失败");
        }
    }

    /**
     * 读取HDFS目录信息（测试完成）
     * @param path 目录路径
     * @return
     * @throws Exception
     */
    @PostMapping("/readPathInfo")
    public Result readPathInfo(@RequestParam("path") String path) throws Exception {
        List<Map<String, Object>> list = hdfsService.readPathInfo(path);
        return new Result("SUCCESS", "读取HDFS目录信息成功", list);
    }

    /**
     * 获取HDFS文件在集群中的位置
     * @param path 包含文件的路径
     * @return
     * @throws Exception
     */
    @PostMapping("/getFileBlockLocations")
    public Result getFileBlockLocations(@RequestParam("path") String path) throws Exception {
        BlockLocation[] blockLocations = hdfsService.getFileBlockLocations(path);
        return new Result("SUCCESS", "获取HDFS文件在集群中的位置", blockLocations);
    }

    /**
     * 创建文件（测试完成）
     * @param path 存放文件的目录
     * @return
     * @throws Exception
     */
    @PostMapping("/createFile")
    public Result createFile(@RequestParam("path") String path, @RequestParam("file") MultipartFile file)
            throws Exception {
        if (StringUtils.isEmpty(path) || null == file.getBytes()) {
            return new Result("FAILURE", "请求参数为空");
        }
        hdfsService.createFile(path, file);
        return new Result("SUCCESS", "创建文件成功");
    }

    /**
     * 读取HDFS文件内容
     * @param path
     * @return
     * @throws Exception
     */
    @PostMapping("/readFile")
    public Result readFile(@RequestParam("path") String path) throws Exception {
        String targetPath = hdfsService.readFile(path);
        return new Result("SUCCESS", "读取HDFS文件内容", targetPath);
    }

    /**
     * 读取HDFS文件转换成Byte类型
     * @param path
     * @return
     * @throws Exception
     */
    @PostMapping("/openFileToBytes")
    public Result openFileToBytes(@RequestParam("path") String path) throws Exception {
        byte[] files = hdfsService.openFileToBytes(path);
        return new Result("SUCCESS", "读取HDFS文件转换成Byte类型", files);
    }

    /**
     * 读取HDFS文件装换成User对象
     * @param path
     * @return
     * @throws Exception
     */
    @PostMapping("/openFileToUser")
    public Result openFileToUser(@RequestParam("path") String path) throws Exception {
        User user = hdfsService.openFileToObject(path, User.class);
        return new Result("SUCCESS", "读取HDFS文件装换成User对象", user);
    }

    /**
     * 读取文件列表（测试完成）
     * @param path 目录所在的路径
     * @return
     * @throws Exception
     */
    @PostMapping("/listFile")
    public Result listFile(@RequestParam("path") String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return new Result("FAILURE", "请求参数为空");
        }
        List<Map<String, String>> returnList = hdfsService.listFile(path);
        return new Result("SUCCESS", "读取文件列表成功", returnList);
    }

    /**
     * 重命名文件（测试完成）
     * @param oldName 包含文件路径
     * @param newName
     * @return
     * @throws Exception
     */
    @PostMapping("/renameFile")
    public Result renameFile(@RequestParam("oldName") String oldName, @RequestParam("newName") String newName)
            throws Exception {
        if (StringUtils.isEmpty(oldName) || StringUtils.isEmpty(newName)) {
            return new Result("FAILURE", "请求参数为空");
        }
        boolean isOk = hdfsService.renameFile(oldName, newName);
        if (isOk) {
            return new Result("SUCCESS", "文件重命名成功");
        } else {
            return new Result("FAILURE", "文件重命名失败");
        }
    }

    /**
     * 删除文件（测试完成）
     * @param path
     * @return
     * @throws Exception
     */
    @PostMapping("/deleteFile")
    public Result deleteFile(@RequestParam("path") String path) throws Exception {
        boolean isOk = hdfsService.deleteFile(path);
        if (isOk) {
            return new Result("SUCCESS", "delete file success");
        } else {
            return new Result("FAILURE", "delete file fail");
        }
    }

    /**
     * 上传文件（完成测试，超时）
     * @param path 客户端路径
     * @param uploadPath 服务器路径
     * @return
     * @throws Exception
     */
    @PostMapping("/uploadFile")
    public Result uploadFile(@RequestParam("path") String path, @RequestParam("uploadPath") String uploadPath)
            throws Exception {
        hdfsService.uploadFile(path, uploadPath);
        return new Result("SUCCESS", "upload file success");
    }

    /**
     * 下载文件（测试完成）
     * @param path
     * @param downloadPath
     * @return
     * @throws Exception
     */
    @PostMapping("/downloadFile")
    public Result downloadFile(@RequestParam("path") String path, @RequestParam("downloadPath") String downloadPath)
            throws Exception {
        hdfsService.downloadFile(path, downloadPath);
        return new Result("SUCCESS", "download file success");
    }

    /**
     * HDFS文件复制（测试完成）
     * @param sourcePath
     * @param targetPath
     * @return
     * @throws Exception
     */
    @PostMapping("/copyFile")
    public Result copyFile(@RequestParam("sourcePath") String sourcePath, @RequestParam("targetPath") String targetPath)
            throws Exception {
        hdfsService.copyFile(sourcePath, targetPath);
        return new Result("SUCCESS", "copy file success");
    }

    /**
     * 查看文件是否已存在（测试完成）
     * @param path
     * @return
     * @throws Exception
     */
    @PostMapping("/existFile")
    public Result existFile(@RequestParam("path") String path) throws Exception {
        boolean isExist = hdfsService.existFile(path);
        return new Result("SUCCESS", "file isExist: " + isExist);
    }
}
