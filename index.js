const path = require("path");
const { Writable } = require("stream");
const COS = require("cos-nodejs-sdk-v5");

const from = {
  SecretId: "",
  SecretKey: "",
  Region: "",
  Bucket: "",
};

const to = {
  SecretId: "",
  SecretKey: "",
  Region: "",
  Bucket: "",
};

const fromCos = new COS({ SecretId: from.SecretId, SecretKey: from.SecretKey });
const toCos = new COS({ SecretId: to.SecretId, SecretKey: to.SecretKey });

const tasks = [
  {
    fromPrefix: "foo/bar",
    toPrefix: "p/foo/bar",
  },
];

const getTaskFiles = (task) => {
  return new Promise((resolve, reject) => {
    let files = [];
    const listFolder = (marker) => {
      fromCos.getBucket(
        {
          Bucket: from.Bucket,
          Region: from.Region,
          Prefix: task.fromPrefix,
          Marker: marker,
          MaxKeys: 100,
          Delimiter: "",
        },
        (err, data) => {
          if (err) {
            console.log("listFolder error", err);
            reject(err);
          } else {
            const newFiles = data.Contents.filter(
              (item) => !item.Key.endsWith("/")
            ).map((file) => ({
              ...file,
              newKey: path.join(
                task.toPrefix,
                path.relative(task.fromPrefix, file.Key)
              ),
            }));
            files = files.concat(newFiles);

            if (data.IsTruncated === "true") {
              listFolder(data.NextMarker);
            } else {
              resolve(files);
              console.log("task files length", task, files.length);
            }
          }
        }
      );
    };
    listFolder();
  });
};

tasks
  .reduce(async (promise, task) => {
    const result = await promise;
    const taskFiles = await getTaskFiles(task);
    return result.concat(taskFiles);
  }, Promise.resolve([]))
  .then((allFiles) => {
    let doneCount = 0;
    const doTransfer = (file) => {
      if (!file) {
        return;
      }
      return new Promise((resolve, reject) => {
        // 同一个存储桶之前使用复制接口
        if (from.Bucket === to.Bucket && from.Region === to.Region) {
          fromCos.sliceCopyFile(
            {
              Bucket: from.Bucket,
              Region: from.Region,
              Key: file.newKey,
              CopySource: `${from.Bucket}.cos.${from.Region}.myqcloud.com/${file.Key}`,
            },
            function (err, data) {
              if (err) {
                reject(err);
                console.log("复制失败", file.Key, err);
              } else {
                console.log(
                  "复制成功",
                  `${++doneCount} ${file.Key} ---> ${file.newKey}`
                );
                resolve(data);
              }
            }
          );
        } else {
          // 跨存储桶先下载再上传
          let buffer = Buffer.from("");
          class CosWriteStream extends Writable {
            _write(chunk, encoding, cb) {
              buffer = Buffer.concat([buffer, chunk]);
              process.nextTick(cb);
            }
          }
          const writeStream = new CosWriteStream();

          fromCos.getObject(
            {
              Bucket: from.Bucket,
              Region: from.Region,
              Key: file.Key,
              Output: writeStream,
            },
            function (err, data) {
              if (err) {
                reject(err);
                console.log("下载失败", file.Key, err);
              } else {
                // console.log("下载成功", file.Key);
                toCos.putObject(
                  {
                    Bucket: to.Bucket,
                    Region: to.Region,
                    Key: file.newKey,
                    Body: buffer,
                  },
                  function (err, data) {
                    if (err) {
                      reject(err);
                      console.log("上传失败", file.newKey, err);
                    } else {
                      console.log(
                        "转移成功",
                        `${++doneCount} ${file.Key} ---> ${file.newKey}`
                      );
                      resolve(data);
                    }
                  }
                );
              }
            }
          );
        }
      }).then(() => doTransfer(allFiles.shift()));
    };

    for (let i = 0; i < 20; i++) {
      doTransfer(allFiles.shift());
    }
  });
