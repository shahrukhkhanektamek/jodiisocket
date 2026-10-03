const { Client } = require('ssh2');

const conn = new Client();

const config = {
  host: '72.62.194.69',
  port: 22,
  username: 'root',
  password: '12aHeQVhJ+WfNuTN@8lH',
};

const cmd = process.argv[2] || 'uname -a; whoami; which node; node -v; which npm; npm -v; which pm2; ufw status; ss -tuln';

conn.on('ready', () => {
  console.log('✅ SSH Connection established to 72.62.194.69');
  conn.exec(cmd, (err, stream) => {
    if (err) throw err;
    stream.on('close', (code, signal) => {
      console.log(`Command closed with code ${code}`);
      conn.end();
    }).on('data', (data) => {
      process.stdout.write(data);
    }).stderr.on('data', (data) => {
      process.stderr.write(data);
    });
  });
}).on('error', (err) => {
  console.error('❌ SSH Error:', err);
}).connect(config);
