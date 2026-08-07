# ruff: noqa: E501

DASHBOARD_HTML = r"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8"><meta name="viewport"
content="width=device-width,initial-scale=1"><title>Информационный помощник по рынку</title>
<style>
body{font:15px system-ui;max-width:1200px;margin:2rem auto;padding:0 1rem;
background:#0d1117;color:#e6edf3}input,select,button{padding:.55rem;margin:.25rem;
background:#161b22;color:#e6edf3;border:1px solid #30363d;border-radius:6px}
button{cursor:pointer;background:#238636}table{width:100%;border-collapse:collapse;
margin-top:1rem}th,td{padding:.6rem;border-bottom:1px solid #30363d;text-align:left}
.warning{color:#d29922}.error{color:#f85149}.muted{color:#8b949e}</style></head>
<body><h1>Информационный помощник по рынку</h1>
<p class="muted">Информационный анализ, не торговая рекомендация.</p>
<form id="screen"><label>Инструменты <input id="symbols" value="BTCUSDT ETHUSDT SOLUSDT"
aria-label="Инструменты"><select id="interval"><option>5m</option><option>15m</option>
<option selected>1h</option><option>4h</option><option>1d</option></select></label>
<label>Минимальный балл <input id="score" type="number" min="0" max="100" value="45"></label>
<label>Минимальная уверенность <input id="confidence" type="number" min="0" max="100" value="0"></label>
<label>Максимум результатов <input id="maximum-results" type="number" min="1" value="50"></label>
<label><input id="derivatives" type="checkbox"> учитывать деривативы</label>
<button>Запустить сканирование</button></form>
<label>Направление <select id="direction"><option>ВСЕ</option><option>ЛОНГ</option>
<option>ШОРТ</option><option>НЕЙТРАЛЬНО</option></select></label>
<div id="meta"></div><div id="empty"></div><table><thead><tr><th>Инструмент</th><th>Направление</th>
<th>Техническая сила сигнала</th><th>Деривативы</th><th>Итоговый балл</th><th>Уверенность</th>
<th>Режим</th><th>Подтверждения</th><th>Причины и предупреждения</th></tr></thead>
<tbody id="results"></tbody></table><h2>Ошибки анализа</h2><div id="errors"></div>
<script>
const addCell=(row,value)=>{const cell=document.createElement('td');
cell.textContent=String(value);row.append(cell);return cell};
const num=value=>Number(value).toLocaleString('ru-RU',
{minimumFractionDigits:1,maximumFractionDigits:1});
let signals=[]; function render(){const filter=document.querySelector('#direction').value;
const table=document.querySelector('#results');table.replaceChildren();
const visible=signals.filter(x=>filter==='ВСЕ'||x.direction===filter);
document.querySelector('#empty').textContent=visible.length?'':'Подходящих сигналов не найдено.';
for(const signal of visible){
const row=document.createElement('tr');addCell(row,signal.symbol);addCell(row,signal.direction);
addCell(row,num(signal.technical_score));addCell(row,signal.derivatives_score===null?'—':num(signal.derivatives_score));
addCell(row,num(signal.combined_score));addCell(row,num(signal.confidence)+'%');
addCell(row,signal.regime);addCell(row,signal.confirmations);
const detail=addCell(row,signal.derivatives_context+' '+signal.explanations.join('; '));
const warning=document.createElement('div');warning.className='warning';
warning.textContent=signal.warnings.join('; ')||'Риски не выявлены.';
detail.append(warning);table.append(row)}}
document.querySelector('#direction').onchange=render;
document.querySelector('#screen').onsubmit=async e=>{e.preventDefault();let symbols=
document.querySelector('#symbols').value.trim().split(/\s+/).filter(Boolean);
let response=await fetch('/api/screen',{method:'POST',headers:{'Content-Type':'application/json'},
body:JSON.stringify({instruments:symbols.map(x=>x+':crypto'),interval:
document.querySelector('#interval').value,minimum_score:+document.querySelector('#score').value,
minimum_confidence:+document.querySelector('#confidence').value,include_derivatives:
document.querySelector('#derivatives').checked,maximum_results:
+document.querySelector('#maximum-results').value})});let data=await response.json();
if(!response.ok){document.querySelector('#errors').textContent=data.error;return}signals=data.ranked_signals;
document.querySelector('#meta').textContent='Сформировано: '+data.generated_at;
const errors=document.querySelector('#errors');errors.replaceChildren();
for(const failure of data.failed_instruments){const item=document.createElement('p');
item.className='error';item.textContent=`${failure.symbol} [${failure.stage}]: ${failure.message}`;
errors.append(item)}render()};
</script></body></html>"""
