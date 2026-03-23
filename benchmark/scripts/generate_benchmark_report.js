const obs=new IntersectionObserver(es=>{es.forEach(e=>{
  const a=document.querySelector('nav.toc a[href="#'+e.target.id+'"]');
  if(a){a.style.borderLeftColor=e.isIntersecting?'var(--accent)':'transparent';
       a.style.color=e.isIntersecting?'var(--accent)':'';}
});},{rootMargin:'-10% 0px -80% 0px'});
document.querySelectorAll('h2[id],h3[id]').forEach(h=>obs.observe(h));

