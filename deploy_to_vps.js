const { Client } = require('ssh2');
const fs = require('fs');
const path = require('path');

const conn = new Client();
const config = {
  host: '72.62.194.69',
  port: 22,
  username: 'root',
  password: '12aHeQVhJ+WfNuTN@8lH',
};

const localFile = path.join(__dirname, 'server.js');
const remoteFile = '/var/www/jodiisocket/server.js';

conn.on('ready', () => {
  console.log('✅ SSH Connected to 72.62.194.69');
  conn.sftp((err, sftp) => {
    if (err) throw err;
    console.log(`Uploading ${localFile} to ${remoteFile}...`);
    sftp.fastPut(localFile, remoteFile, (uploadErr) => {
      if (uploadErr) throw uploadErr;
      console.log('✅ Upload successful! Restarting PM2...');
      conn.exec('pm2 restart jodiisocket; pm2 save; ss -tulpn | grep 3001', (execErr, stream) => {
        if (execErr) throw execErr;
        stream.on('close', (code) => {
          console.log(`PM2 restart finished with code ${code}`);
          conn.end();
        }).on('data', (d) => process.stdout.write(d))
          .stderr.on('data', (d) => process.stderr.write(d));
      });
    });
  });
}).on('error', (err) => {
  console.error('❌ SSH Error:', err);
}).connect(config);
