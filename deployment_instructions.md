✅ Option 1: Explicitly include .env + everything else
Use PowerShell from your project root:

powershell
Copy
Edit
scp -r * root@159.223.104.254:/root/bill_review
scp .env root@159.223.104.254:/root/bill_review/.env


connect to vm
ssh root@159.223.104.254



✅ Step 2: Create a tmux session for your app
bash
Copy
Edit
tmux new -s billapp
You’ll now be inside a tmux session named billapp.



✅ Step 3: Activate your venv and run the app
bash
Copy
Edit
cd ~/bill_review
source venv/bin/activate
python -m portal.run
Leave that running — now press:

bash
Copy
Edit
Ctrl + B, then press D
That detaches the tmux session — your app is still running in the background 🎉



✅ Step 4: Open a new tmux session for ngrok
bash
Copy
Edit
tmux new -s ngrok
Then run your reserved domain tunnel:

bash
Copy
Edit
ngrok http 5002 --hostname=cdx-billreview.ngrok.io --authtoken=2tMT7cxxHUtpVl6pmdDxx7Y8vdy_79uYXhPyZZdpRGoZ2hFoc


ngrok http 5002 --domain=cdx-billreview.ngrok.io --authtoken=cr_2tMT7cxxHUtpVl6pmdDxx7Y8vdy
Once it says:

nginx
Copy
Edit
Forwarding https://cdx-billreview.ngrok.io -> http://localhost:5002
You're live.

Detach again:

bash
Copy
Edit
Ctrl + B, then D
✅ Now You Can Safely Close Your Laptop
The Flask app is running in the billapp tmux session

ngrok is tunneling in the ngrok session

Your droplet is now fully independent of your laptop

