# Running the cluster on Google Cloud (instead of 3 PCs)

Use this guide if you don't have three Ubuntu PCs. You rent three Linux machines on Google Cloud and name them `master`, `worker1` and `worker2`, just like the PCs in [CLUSTER_SETUP.md](CLUSTER_SETUP.md). Once step 5 is done, it is a normal GraphPulse cluster, and [COMMANDS.md](COMMANDS.md) and [DEMO_SCRIPT.md](DEMO_SCRIPT.md) work unchanged.

Compared with the PC setup, a few things are simpler. You don't set hostnames, edit `/etc/hosts`, create users or type passwords, because Google Cloud handles those. Step 3 replaces `setup_ssh.sh`.

> **Before you start**
> - Ask your instructor whether cloud machines count as the "1 master + 2 worker PCs" in the project brief.
> - Status: written on 13 Nov 2026 and **not yet run end to end**. If a step fails, copy the last ~40 lines of output and send them to the team.

## What it costs

| Machine | Google Cloud type | Size | Price (Mumbai region) |
|---|---|---|---|
| `master` | e2-standard-4 | 4 vCPU, 16 GB RAM | about $0.16 per hour |
| `worker1`, `worker2` | e2-standard-2 | 2 vCPU, 8 GB RAM each | about $0.08 per hour each |

- **Running cost:** about **$0.35 an hour** for all three while they run, so even a full day is under $10. Their disks add roughly $0.30 a day, and that continues while the machines are stopped.
- **Free credit:** new Google Cloud accounts get **$300 of free credit for 90 days**.
- **No surprise bills:** on the free trial you are never billed. If the credit runs out, the machines are stopped, and they are deleted 30 days later unless you upgrade.

The workers match the 8 GB PCs this project was sized for (`cluster/config/hadoop/yarn-site.xml`). The master gets 16 GB because it runs about eight Java services plus the Spark driver and the dashboard, and 8 GB is tight for that.

---

## Step 1: Create the account (browser, ~10 min)

1. Go to <https://console.cloud.google.com> and sign in. **Use a personal Gmail account.** College accounts can block the login method used in step 3.
2. Start the free trial. It asks for a card or other payment method to check you're a real person. You are not charged unless you upgrade to a paid account yourself.
3. At the top of the page, make sure a project is selected. New accounts get "My First Project".
4. Click **Activate Cloud Shell**, the `>_` icon at the top right. A Linux terminal opens at the bottom of the page. All "Cloud Shell" commands below go there.

## Step 2: Create the three machines (Cloud Shell, ~5 min)

First find your laptop's public IP address: open <https://api.ipify.org> in your browser. Then paste this into Cloud Shell, replacing `YOUR_IP` with that address:

```bash
gcloud config get-value project          # must print your project id (see Troubleshooting if empty)
gcloud config set compute/region asia-south1
gcloud config set compute/zone asia-south1-a
gcloud services enable compute.googleapis.com

# Only your laptop may open the web pages (dashboard 8000, YARN 8088, HDFS 9870, HBase 16010, Spark history 18080)
gcloud compute firewall-rules create graphpulse-web --network=default \
  --allow=tcp:8000,tcp:8088,tcp:9870,tcp:16010,tcp:18080 \
  --source-ranges=YOUR_IP/32 --target-tags=graphpulse

COMMON="--image-family=ubuntu-2204-lts --image-project=ubuntu-os-cloud --boot-disk-size=30GB --boot-disk-type=pd-balanced --tags=graphpulse"
gcloud compute instances create master --machine-type=e2-standard-4 $COMMON
gcloud compute instances create worker1 worker2 --machine-type=e2-standard-2 $COMMON
gcloud compute instances list
```

The last command lists the three machines with their `EXTERNAL_IP`, which you'll need in step 7.

The machines can already talk to each other. Google's `default` network allows all traffic between them, and each machine finds the others by name. All three must be in the **same zone** for that to work.

## Step 3: Create the `hadoop` user and connect the machines (Cloud Shell, ~3 min)

Wait about a minute after step 2 so the machines finish booting, then run:

```bash
gcloud compute ssh hadoop@master --command="whoami"
```

The first time, it asks for a passphrase. **Press Enter twice to leave it empty.** This creates an SSH key and a user called `hadoop` (with admin rights) on all three machines. It should print `hadoop`.

Then give master the same key, so it can log in to the workers. Hadoop's start scripts need this:

```bash
gcloud compute scp ~/.ssh/google_compute_engine hadoop@master:.ssh/id_rsa
gcloud compute scp ~/.ssh/google_compute_engine.pub hadoop@master:.ssh/id_rsa.pub
```

## Step 4: Install everything (on master, ~30–60 min, mostly downloads)

Log in to master as `hadoop`:

```bash
gcloud compute ssh hadoop@master
```

You are now on master. Check that it can reach every machine. It should print `master`, `worker1` and `worker2`:

```bash
chmod 600 ~/.ssh/id_rsa
for h in master worker1 worker2; do ssh -o StrictHostKeyChecking=accept-new $h hostname; done
```

Install master:

```bash
sudo apt-get update -qq && sudo apt-get install -y -qq git
git clone https://github.com/Anirudh1621/graphpulse ~/graphpulse && cd ~/graphpulse
./cluster/scripts/install_node.sh master
pip3 install --user pandas "numpy<2"     # job 02 needs pandas; harmless once the fix is in GitHub
source /etc/profile.d/bigdata.sh
```

Install both workers from master. This copies master's downloaded files over Google's fast internal network instead of downloading them again:

```bash
for h in worker1 worker2; do
  scp -r ~/gp-downloads $h:
  ssh -t $h "sudo apt-get update -qq && sudo apt-get install -y -qq git && \
    git clone https://github.com/Anirudh1621/graphpulse ~/graphpulse && \
    ~/graphpulse/cluster/scripts/install_node.sh $h && pip3 install --user pandas 'numpy<2'"
done
```

If a purple screen asks which services to restart, press Enter.

## Step 5: Start the cluster (on master)

```bash
cd ~/graphpulse
./cluster/scripts/bootstrap.sh
```

Every line of the status table at the end should say **UP**.

## Step 6: Run the batch pipeline (on master)

Long jobs are started with `nohup … &`, so they keep running even if Cloud Shell disconnects. Start with the small dataset as a quick test:

```bash
./scripts/download_data.sh --small
nohup ./scripts/run_pipeline.sh > ~/pipeline.log 2>&1 &
tail -f ~/pipeline.log        # Ctrl+C stops watching, not the job
```

When the log says `Batch pipeline finished`, switch to the full Twitter data:

```bash
hdfs dfs -rm -r /graphpulse/raw/edges/facebook_combined.txt
./scripts/download_data.sh
nohup ./scripts/run_pipeline.sh > ~/pipeline.log 2>&1 &
tail -f ~/pipeline.log
```

## Step 7: Live layer and dashboard (on master)

The dashboard is built on master, which needs Node.js. Install it once:

```bash
sudo snap install node --classic --channel=22
```

Then start the three long-running parts:

```bash
cd ~/graphpulse
nohup ./scripts/run_stream.sh > ~/stream.log 2>&1 &
nohup python3 producer/event_producer.py --edges data/twitter_combined.txt --rate 30 > ~/producer.log 2>&1 &
nohup ./scripts/run_api.sh > ~/api.log 2>&1 &
tail -f ~/api.log             # wait for "Application startup complete"
```

The first API start takes a few minutes because it builds the dashboard.

On your laptop, open `http://MASTER_EXTERNAL_IP:8000`, using master's `EXTERNAL_IP` from `gcloud compute instances list`. The Hadoop pages use the same address with their own port: `:9870` (HDFS), `:8088` (YARN), `:16010` (HBase) and `:18080` (Spark history).

*Optional:* the dashboard's Cluster page links to `http://master:9870` and so on. To make those links work from your laptop, add the line `MASTER_EXTERNAL_IP master` to `C:\Windows\System32\drivers\etc\hosts`. Editing that file needs admin rights.

## Step 8: Stop paying when you're done (Cloud Shell)

**Pause.** This keeps all data, and only the disks are billed while paused:

```bash
gcloud compute ssh hadoop@master --command="~/graphpulse/cluster/scripts/stop_all.sh"
gcloud compute instances stop master worker1 worker2
```

**Resume:**

```bash
gcloud compute instances start master worker1 worker2
gcloud compute ssh hadoop@master --command="~/graphpulse/cluster/scripts/start_all.sh"
```

After a resume, run step 7's three `nohup` commands again. Master's external IP usually changes after a restart, so check `gcloud compute instances list`.

**Delete everything for good:**

```bash
gcloud compute instances delete master worker1 worker2 --quiet
gcloud compute firewall-rules delete graphpulse-web --quiet
```

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `gcloud config get-value project` prints nothing, or `The required property [project] is not currently set` | Run `gcloud projects list`, then `gcloud config set project <PROJECT_ID>` |
| Quota error when creating the machines | Make master smaller: `gcloud compute instances delete master --quiet`, then create it again with `--machine-type=e2-standard-2` |
| `does not have enough resources available` | Delete any machines already created, run `gcloud config set compute/zone asia-south1-b`, and create all three again. They must share one zone. |
| `Connection refused` right after creating the machines | They are still booting. Wait a minute and try again. |
| `whoami` doesn't print `hadoop` | OS Login is switched on. Run `gcloud compute project-info add-metadata --metadata=enable-oslogin=FALSE` and repeat step 3. If that doesn't help, your account is managed by an organisation (for example a college account): use a personal Gmail account. |
| `Could not get lock /var/lib/dpkg/lock-frontend` | The machine is still updating itself after its first boot. Wait 2 minutes and run the same command again. |
| Re-running step 4 says `destination path ... already exists` | The download already worked, so skip `git clone`. On master, run `cd ~/graphpulse` and continue from `install_node.sh`. For a worker, run `ssh -t worker1 "~/graphpulse/cluster/scripts/install_node.sh worker1 && pip3 install --user pandas 'numpy<2'"` (likewise for worker2). |
| Dashboard doesn't load in your browser | Your laptop's IP changed, for example on a new Wi-Fi network. Run `gcloud compute firewall-rules update graphpulse-web --source-ranges=NEW_IP/32` |
| Cluster page lists machines under long names like `worker1.asia-south1-a.c.<project>.internal` | Only cosmetic: that's Google's full name for the machine |
| Anything else | The troubleshooting table in [CLUSTER_SETUP.md](CLUSTER_SETUP.md) applies here too |
