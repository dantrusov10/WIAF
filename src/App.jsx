
import {useState} from 'react'

export default function App(){

const [tab,setTab] = useState("dashboard")

return(
<div>

<nav className="nav">
<div><strong>WIAF</strong></div>
<div>Цифровые торги на международную логистику</div>
</nav>

<section className="hero">
<div className="container">
<h1>Снизьте стоимость международной логистики через прозрачные цифровые торги</h1>
<p>Импортёры получают лучшие ставки от проверенных экспедиторов. Экспедиторы получают доступ к новым сделкам и прозрачным правилам торгов.</p>
<a className="cta">Создать аукцион</a>
</div>
</section>

<section className="section">
<div className="container">
<h2>Как работает платформа</h2>
<div className="cards">

<div className="card">
<h3>Создайте аукцион</h3>
<p>Опишите маршрут, груз и сроки доставки. Платформа автоматически публикует запрос для экспедиторов.</p>
</div>

<div className="card">
<h3>Экспедиторы делают ставки</h3>
<p>Компании соревнуются за заказ. Каждая новая ставка снижает стоимость логистики.</p>
</div>

<div className="card">
<h3>Выберите победителя</h3>
<p>Контакт открывается только победителю торгов. Все правила прозрачны и фиксируются в системе.</p>
</div>

</div>
</div>
</section>

<section className="section">
<div className="container">

<h2>Пример интерфейса платформы</h2>

<div className="dashboard-tabs">
<div className="tab" onClick={()=>setTab("dashboard")}>Дашборд</div>
<div className="tab" onClick={()=>setTab("auctions")}>Мои аукционы</div>
<div className="tab" onClick={()=>setTab("templates")}>Шаблоны</div>
<div className="tab" onClick={()=>setTab("analytics")}>Аналитика</div>
</div>

{tab==="dashboard" && (
<div className="card">
<h3>Дашборд</h3>
<p>Общий обзор активных торгов, экономии на логистике и статистики ставок.</p>
</div>
)}

{tab==="auctions" && (
<div className="card">
<h3>Мои аукционы</h3>
<p>Список текущих и завершённых торгов, ставки экспедиторов и победители.</p>
</div>
)}

{tab==="templates" && (
<div className="card">
<h3>Шаблоны</h3>
<p>Сохраняйте типовые маршруты и грузы, чтобы запускать новые торги за секунды.</p>
</div>
)}

{tab==="analytics" && (
<div className="card">
<h3>Аналитика</h3>
<p>Отслеживайте динамику ставок и среднюю стоимость логистики по маршрутам.</p>
</div>
)}

</div>
</section>

<section className="section">
<div className="container">

<h2>Почему компании используют WIAF</h2>

<div className="cards">

<div className="card">
<h3>Снижение стоимости логистики</h3>
<p>Аукционная модель позволяет получать более конкурентные ставки.</p>
</div>

<div className="card">
<h3>Прозрачные правила</h3>
<p>Каждая ставка фиксируется, а победитель определяется по понятным правилам.</p>
</div>

<div className="card">
<h3>Новые сделки для экспедиторов</h3>
<p>Платформа даёт доступ к запросам импортёров на международные перевозки.</p>
</div>

</div>

<a className="cta">Запустить торги</a>

</div>
</section>

<section className="section">
<div className="container">

<h2>Новости логистики и ВЭД</h2>

<div className="news">
<p>В этом разделе будут публиковаться ключевые новости рынка логистики, международных перевозок и ВЭД.</p>
<p>Планируется интеграция автоматической агрегации новостей и аналитики.</p>
</div>

</div>
</section>

</div>
)
}
