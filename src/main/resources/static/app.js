/* =============== GLOBALS =============== */
const $ = s => document.querySelector(s);

const GUI = {
    api       : '/api',        // same-origin
    nodePort  : 8081,
    refreshMs : 10_000,
    nodes     : []
};

// <remote-ip> : <forwarded local port>
const LAUNCHER = {
    "172.20.0.4": 19090,
    "172.20.0.5": 19091,
    "172.20.0.6": 19092
};

/* =============== HELPERS =============== */
const pane   = id => $(`#tab-${id}`);
const setVis = (el,v) => el.classList.toggle('visible',v);
const card   = html => { const d = document.createElement('div'); d.innerHTML = html; return d.firstElementChild; };
const show   = el => el.classList.remove('hidden');
const hide   = el => el.classList.add('hidden');

async function jfetch(url,opt){
    const r = await fetch(url,opt);
    if(!r.ok) throw new Error(r.statusText);
    return r.json();
}

/* =============== GUI switching =============== */
document.querySelectorAll('.sidebar .tab').forEach(btn=>{
    btn.onclick = ()=>{
        document.querySelectorAll('.sidebar .tab').forEach(b=>b.classList.remove('active'));
        btn.classList.add('active');
        ['home','nodes','files','opts'].forEach(t=>setVis(pane(t),false));
        setVis(pane(btn.dataset.tab),true);
        if(btn.dataset.tab==='files') loadFiles();
    };
});

/* =============== Data loaders =============== */
async function loadNodes(){
    let list = [];
    try {
        list = await jfetch(`${GUI.api}/gui/nodes`);
    } catch {}
    GUI.nodes = list;
    $('#ns-count').textContent = list.length;
    fillHome();
    fillNodesTab();
}

async function loadFiles(){
    const grid = $('#file-grid'); grid.innerHTML = '';
    for(const n of GUI.nodes){
        let files;
        try {
            files = await jfetch(`${GUI.api}/gui/files/${n.hash}`);
        } catch {
            continue;
        }
        const box = card(`
        <div class="card">
          <h3>${n.name}</h3>
          <p><b>Local</b> ${files.local.length}</p>
          <p><b>Replicas</b> ${files.replicas.length}</p>
        </div>`);
        box.onclick = ()=>showFileList(n,files);
        grid.append(box);
    }
}

function showFileList(node,files){
    alert(`${node.name}\n\nLocal:\n${files.local.join('\n')}\n\nReplicas:\n${files.replicas.join('\n')}`);
}

/* =============== Table fillers =============== */
function fillHome(){
    const tbody = $('#tbl-home tbody'); tbody.innerHTML = '';
    for(const n of GUI.nodes){
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td>${n.name}</td>
            <td>${n.hash}</td>
            <td>${n.ip}</td>
            <td>${n.prev}</td>
            <td>${n.next}</td>`;
        tr.onclick = ()=>showDetail(n);
        tbody.append(tr);
    }
}

function fillNodesTab(){
    const tbody = $('#tbl-nodes tbody'); tbody.innerHTML = '';
    for(const n of GUI.nodes){
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td>${n.name}</td>
            <td>${n.hash}</td>
            <td>${n.ip}</td>
            <td>ACTIVE</td>
            <td></td>`;
        tbody.append(tr);
    }
}

function showDetail(n){
    const d = $('#detail'); d.innerHTML = '';
    d.append(card(`
        <h3>${n.name}</h3>
        <p>ID ${n.hash}<br>IP ${n.ip}<br>Prev ${n.prev}<br>Next ${n.next}</p>
    `));
    d.classList.remove('hidden');
}

/* =============== Buttons =============== */
$('#btn-refresh').onclick = loadNodes;

$('#btn-add').onclick = async ()=>{
    const nodeName = prompt('Node name?');
    if(!nodeName) return;

    // toon modal
    show($('#host-modal'));

    await new Promise(resolve=>{
        const ok     = $('#host-ok');
        const cancel = $('#host-cancel');
        const sel    = $('#sel-host');

        const cleanup = ()=>{
            ok.onclick = cancel.onclick = null;
            hide($('#host-modal'));
            resolve();
        };

        cancel.onclick = cleanup;

        ok.onclick = async ()=>{
            const ip   = sel.value;
            const port = LAUNCHER[ip];
            if(!port){
                alert('Unknown host');
                cleanup();
                return;
            }

            try {
                await jfetch(`http://localhost:${port}/launch`,{
                    method :'POST',
                    headers:{'Content-Type':'application/json'},
                    body   : JSON.stringify({
                        nodeName,
                        namingServer:'http://172.20.0.3:8080'
                    })
                });
            } catch(e){
                alert('Launch failed: '+e.message);
            }
            // force refresh na 6s
            setTimeout(loadNodes,6000);
            cleanup();
        };
    });
};

/* =============== Theme selector =============== */
$('#sel-theme').onchange = e=>{
    document.documentElement.classList.toggle('dark', e.target.value==='dark');
};

/* =============== Kick-off =============== */
(async ()=>{
    // zet naming-server IP vast
    $('#ns-ip').textContent = '172.20.0.3';

    loadNodes();
    setInterval(loadNodes, GUI.refreshMs);
})();
