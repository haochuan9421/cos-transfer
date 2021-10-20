const path = require("path");
const { Writable } = require("stream");
const COS = require("cos-nodejs-sdk-v5");

const from = {
  SecretId: "",
  SecretKey: "",
  Region: "ap-guangzhou",
  Bucket: "szcloudplus-1302968899",
};

const to = {
  SecretId: "",
  SecretKey: "",
  Region: "ap-guangzhou",
  Bucket: "cdn-1302968899",
};

const fromCos = new COS({ SecretId: from.SecretId, SecretKey: from.SecretKey });
const toCos = new COS({ SecretId: to.SecretId, SecretKey: to.SecretKey });

const tasks = [
  {
    fromPrefix: "export/wzt-wj-server",
    toPrefix: "p/wzt-wj-server/export",
  },
  {
    fromPrefix: "public",
    toPrefix: "p/wzt-wj-server/assets",
  },
  {
    fromPrefix: "public",
    toPrefix: "p/wj-server/assets",
  },
  {
    fromPrefix: "upload/wzt-wj-server",
    toPrefix: "p/wzt-wj-server/upload",
  },
  {
    fromPrefix: "upload/wj-server",
    toPrefix: "p/wj-server/upload",
  },
];

function getAllOldFiles(task) {
  return new Promise((resolve, reject) => {
    let files = [];
    const listFolder = function (marker) {
      fromCos.getBucket(
        {
          Bucket: from.Bucket,
          Region: from.Region,
          Prefix: task.fromPrefix,
          Marker: marker,
          MaxKeys: 100,
          Delimiter: "",
        },
        function (err, data) {
          if (err) {
            return console.log("listFolder error", reject(err));
          } else {
            files = files.concat(
              data.Contents.filter((item) => !item.Key.endsWith("/"))
            );
            if (data.IsTruncated === "true") {
              listFolder(data.NextMarker);
            } else {
              resolve(files);
              console.log("allFiles length", task, files.length);
            }
          }
        }
      );
    };
    listFolder();
  });
}

tasks.reduce(async (promise, task) => {
  await promise;
  const allFiles = await getAllOldFiles(task);
  await allFiles.reduce(async (promise, file, index) => {
    await promise;
    await new Promise((resolve, reject) => {
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
            const newKey = path.join(
              task.toPrefix,
              path.relative(task.fromPrefix, file.Key)
            );
            toCos.putObject(
              {
                Bucket: to.Bucket,
                Region: to.Region,
                Key: newKey,
                Body: buffer,
              },
              function (err, data) {
                if (err) {
                  reject(err);
                  console.log("上传失败", newKey, err);
                } else {
                  console.log(
                    "转移成功",
                    `${index + 1} ${file.Key} ---> ${newKey}`
                  );
                  resolve(data);
                }
              }
            );
          }
        }
      );
    });
  }, Promise.resolve());
}, Promise.resolve());
