/**
 * Programmatic SEO discovery: sitemap index + chunked employer sitemaps,
 * and HTML browse hub / letter pages with internal links to /h1b-employer/:slug.
 */

const SITEMAP_MAX_URLS = 45000;
const BROWSE_PAGE_SIZE = 250;
const BROWSE_MAX_PAGE = 5000;
const XML_CACHE = "public, max-age=3600";
const HTML_CACHE = "public, max-age=1800";

const STATIC_SITEMAP_PATHS = [
  "/",
  "/employers/",
  "/insights/",
  "/insights/list-of-h1b-concurrent-employers-2026/",
  "/reach-out",
];

const BROWSE_KEYS = [
  ..."abcdefghijklmnopqrstuvwxyz".split(""),
  ..."0123456789".split(""),
  "other",
];

function originFromRequest(request) {
  return new URL(request.url).origin;
}

function escapeXml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function escapeHtml(s) {
  return String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function escapeHtmlAttr(s) {
  return String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/"/g, "&quot;")
    .replace(/</g, "&lt;");
}

/** @returns {{ type: 'hub' } | { type: 'letter', key: string } | null} */
export function parseBrowseEmployersPath(pathname) {
  let p = pathname;
  if (p.endsWith("/") && p.length > 1) p = p.slice(0, -1);
  if (p === "/employers") return { type: "hub" };
  const m = p.match(/^\/employers\/(other|[a-z0-9])$/);
  if (!m) return null;
  return { type: "letter", key: m[1] };
}

function employerWhereClause(key) {
  if (key === "other") {
    return {
      sql: `(substr(employer_name, 1, 1) NOT BETWEEN 'a' AND 'z') AND (substr(employer_name, 1, 1) NOT BETWEEN '0' AND '9')`,
      binds: [],
    };
  }
  return {
    sql: `substr(employer_name, 1, 1) = ?`,
    binds: [key],
  };
}

function letterHeading(key) {
  if (key === "other") return "Employers (names not starting with A–Z or 0–9)";
  if (/^\d$/.test(key)) return `Employers starting with “${key}”`;
  return `Employers starting with “${key.toUpperCase()}”`;
}

function xmlResponse(body, status = 200) {
  return new Response(body, {
    status,
    headers: {
      "Content-Type": "application/xml; charset=utf-8",
      "Cache-Control": XML_CACHE,
    },
  });
}

async function serveSitemapIndex(request, env) {
  if (!env.DB) {
    return new Response("Not available", { status: 503 });
  }
  try {
    const session = env.DB.withSession();
    const row = await session.prepare("SELECT COUNT(*) AS c FROM employer_seo").first();
    const total = Number(row?.c ?? 0);
    const origin = originFromRequest(request);
    const pages = total > 0 ? Math.ceil(total / SITEMAP_MAX_URLS) : 0;

    let xml = `<?xml version="1.0" encoding="UTF-8"?>\n`;
    xml += `<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n`;
    xml += `  <sitemap><loc>${escapeXml(`${origin}/sitemap-static.xml`)}</loc></sitemap>\n`;
    for (let i = 1; i <= pages; i++) {
      xml += `  <sitemap><loc>${escapeXml(`${origin}/sitemap-employers-${i}.xml`)}</loc></sitemap>\n`;
    }
    xml += `</sitemapindex>`;
    return xmlResponse(xml);
  } catch (e) {
    console.error("sitemap index", e?.message || e);
    return new Response("Server error", { status: 500 });
  }
}

function serveSitemapStatic(request) {
  const origin = originFromRequest(request);
  let xml = `<?xml version="1.0" encoding="UTF-8"?>\n`;
  xml += `<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n`;
  for (const path of STATIC_SITEMAP_PATHS) {
    const loc = path.startsWith("http") ? path : `${origin}${path}`;
    xml += `  <url><loc>${escapeXml(loc)}</loc><changefreq>weekly</changefreq><priority>${path === "/" ? "1.0" : "0.85"}</priority></url>\n`;
  }
  xml += `</urlset>`;
  return xmlResponse(xml);
}

async function serveSitemapEmployersChunk(request, env, pageNum) {
  if (!env.DB || !Number.isFinite(pageNum) || pageNum < 1) {
    return new Response("Not found", { status: 404 });
  }
  try {
    const session = env.DB.withSession();
    const countRow = await session.prepare("SELECT COUNT(*) AS c FROM employer_seo").first();
    const total = Number(countRow?.c ?? 0);
    const maxPage = total > 0 ? Math.ceil(total / SITEMAP_MAX_URLS) : 0;
    if (maxPage === 0) {
      let empty = `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n</urlset>`;
      return xmlResponse(empty);
    }
    if (pageNum > maxPage) {
      let empty = `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n</urlset>`;
      return xmlResponse(empty);
    }
    const offset = (pageNum - 1) * SITEMAP_MAX_URLS;
    const rows = await session
      .prepare(
        "SELECT slug FROM employer_seo ORDER BY slug LIMIT ? OFFSET ?"
      )
      .bind(SITEMAP_MAX_URLS, offset)
      .all();
    const origin = originFromRequest(request);
    const list = rows?.results || [];
    let xml = `<?xml version="1.0" encoding="UTF-8"?>\n`;
    xml += `<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n`;
    for (const r of list) {
      const slug = r.slug;
      if (!slug) continue;
      const loc = `${origin}/h1b-employer/${slug}`;
      xml += `  <url><loc>${escapeXml(loc)}</loc><changefreq>monthly</changefreq><priority>0.65</priority></url>\n`;
    }
    xml += `</urlset>`;
    return xmlResponse(xml);
  } catch (e) {
    console.error("sitemap employers chunk", e?.message || e);
    return new Response("Server error", { status: 500 });
  }
}

function browseShell({ title, description, canonical, jsonLd, bodyHtml }) {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<script async src="https://www.googletagmanager.com/gtag/js?id=G-FB88SFVP3D"></script>
<script src="/js/gtag.js"></script>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link rel="icon" type="image/svg+xml" href="/favicon.svg">
<title>${escapeHtml(title)}</title>
<meta name="description" content="${escapeHtmlAttr(description)}">
<meta name="robots" content="index, follow">
<link rel="canonical" href="${escapeHtmlAttr(canonical)}">
<link rel="stylesheet" href="/css/browse-employers.css">
<script type="application/ld+json">${jsonLd}</script>
</head>
<body>
<a href="#main" class="skip-link">Skip to content</a>
<nav class="browse-nav" aria-label="Main navigation">
  <div class="nav-dot"></div>
  <a href="/" class="nav-brand">H1B<span class="nav-accent">Search</span></a>
  <div class="nav-links">
    <a href="/insights/" class="nav-link">Insights</a>
    <span class="nav-sep" aria-hidden="true">·</span>
    <a href="/employers/" class="nav-link">Employers</a>
    <span class="nav-sep" aria-hidden="true">·</span>
    <a href="/" class="nav-link">Salary search</a>
  </div>
</nav>
<main id="main" class="browse-main">
${bodyHtml}
</main>
<footer class="site-footer browse-footer">
  <a href="/">Home</a>
  <span class="footer-sep">&middot;</span>
  <a href="/employers/">Browse employers</a>
  <span class="footer-sep">&middot;</span>
  <a href="/sitemap.xml">Sitemap index</a>
  <span class="footer-sep">&middot;</span>
  <a href="https://github.com/gary-limo/h1b-salary-search-" target="_blank" rel="noopener noreferrer">Open source</a>
</footer>
</body>
</html>`;
}

async function serveBrowseHub(request, env) {
  if (!env.DB) {
    return new Response("Not available", { status: 503 });
  }
  const origin = originFromRequest(request);
  const canonical = `${origin}/employers/`;
  let total = 0;
  try {
    const session = env.DB.withSession();
    const row = await session.prepare("SELECT COUNT(*) AS c FROM employer_seo").first();
    total = Number(row?.c ?? 0);
  } catch (e) {
    console.error("browse hub count", e?.message || e);
    return new Response("Server error", { status: 500 });
  }

  const letterLinks = BROWSE_KEYS.map((key) => {
    const href = key === "other" ? "/employers/other/" : `/employers/${key}/`;
    const label = key === "other" ? "Other" : /^\d$/.test(key) ? key : key.toUpperCase();
    return `<a href="${escapeHtmlAttr(href)}">${escapeHtml(label)}</a>`;
  }).join("\n");

  const jsonLd = JSON.stringify({
    "@context": "https://schema.org",
    "@type": "CollectionPage",
    name: "Browse H-1B employers",
    description:
      "Alphabetical index of employers in the H1B LCA salary database with links to salary pages.",
    url: canonical,
    isPartOf: {
      "@type": "WebSite",
      name: "H1B Salary Search",
      url: `${origin}/`,
    },
    breadcrumb: {
      "@type": "BreadcrumbList",
      itemListElement: [
        { "@type": "ListItem", position: 1, name: "Home", item: `${origin}/` },
        { "@type": "ListItem", position: 2, name: "Employers", item: canonical },
      ],
    },
  });

  const bodyHtml = `
<nav class="browse-breadcrumb" aria-label="Breadcrumb"><a href="/">Home</a> / <span aria-current="page">Employers</span></nav>
<h1>Browse employers</h1>
<p class="lede">Open an employer’s salary page (LCA disclosure data). Use the <a href="/sitemap.xml">XML sitemap index</a> for crawlers listing every employer URL.</p>
<p class="browse-sitemap-note">Discovery: <a href="/sitemap.xml">Sitemap index</a> includes <strong>${escapeHtml(String(total.toLocaleString()))}</strong> employer URLs in chunked sitemaps plus static site URLs.</p>
<h2 class="browse-count" style="font-size:15px;font-weight:700;margin-bottom:10px">By first letter or digit</h2>
<div class="browse-letter-grid" role="navigation" aria-label="Employer first letter">${letterLinks}</div>`;

  const html = browseShell({
    title: "Browse employers | H1B Salary Search",
    description: `Browse ${total.toLocaleString()} employers from U.S. DOL H-1B LCA data. Open salary pages by letter or use the sitemap for full coverage.`,
    canonical,
    jsonLd,
    bodyHtml,
  });

  return new Response(html, {
    status: 200,
    headers: {
      "Content-Type": "text/html; charset=utf-8",
      "Cache-Control": HTML_CACHE,
    },
  });
}

async function serveBrowseLetter(request, env, key, searchParams) {
  if (!env.DB) {
    return new Response("Not available", { status: 503 });
  }
  if (!BROWSE_KEYS.includes(key)) {
    return new Response("Not found", { status: 404 });
  }

  let page = parseInt(searchParams.get("page") || "1", 10) || 1;
  page = Math.min(BROWSE_MAX_PAGE, Math.max(1, page));

  const { sql: whereSql, binds } = employerWhereClause(key);
  const offset = (page - 1) * BROWSE_PAGE_SIZE;

  try {
    const session = env.DB.withSession();
    const countStmt = `SELECT COUNT(*) AS c FROM employer_seo WHERE ${whereSql}`;
    const countRow = await session.prepare(countStmt).bind(...binds).first();
    const total = Number(countRow?.c ?? 0);
    const totalPages = total > 0 ? Math.ceil(total / BROWSE_PAGE_SIZE) : 1;

    const dataStmt = `SELECT slug, employer_name FROM employer_seo WHERE ${whereSql} ORDER BY employer_name LIMIT ? OFFSET ?`;
    const rows = await session
      .prepare(dataStmt)
      .bind(...binds, BROWSE_PAGE_SIZE, offset)
      .all();
    const list = rows?.results || [];

    const origin = originFromRequest(request);
    const basePath = key === "other" ? "/employers/other/" : `/employers/${key}/`;
    const canonical =
      page <= 1 ? `${origin}${basePath}` : `${origin}${basePath}?page=${page}`;

    const jsonLd = JSON.stringify({
      "@context": "https://schema.org",
      "@type": "CollectionPage",
      name: letterHeading(key),
      description: `H-1B LCA employers: ${letterHeading(key)} (${total.toLocaleString()} total).`,
      url: canonical,
      isPartOf: { "@type": "WebSite", name: "H1B Salary Search", url: `${origin}/` },
      breadcrumb: {
        "@type": "BreadcrumbList",
        itemListElement: [
          { "@type": "ListItem", position: 1, name: "Home", item: `${origin}/` },
          { "@type": "ListItem", position: 2, name: "Employers", item: `${origin}/employers/` },
          { "@type": "ListItem", position: 3, name: letterHeading(key), item: canonical },
        ],
      },
    });

    const items = list
      .map((r) => {
        const href = `/h1b-employer/${escapeHtmlAttr(r.slug)}`;
        const label = escapeHtml(r.employer_name || r.slug);
        return `<li><a href="${href}">${label}</a></li>`;
      })
      .join("\n");

    let pagerHtml = "";
    if (totalPages > 1) {
      const parts = [
        `<div class="browse-pager" role="navigation" aria-label="Pagination"><span>Page ${page} of ${totalPages}</span>`,
      ];
      if (page > 1) {
        const prev = page === 2 ? basePath : `${basePath}?page=${page - 1}`;
        parts.push(`<a href="${escapeHtmlAttr(prev)}" rel="prev">Previous</a>`);
      }
      if (page < totalPages) {
        parts.push(
          `<a href="${escapeHtmlAttr(`${basePath}?page=${page + 1}`)}" rel="next">Next</a>`
        );
      }
      parts.push(`<a href="${escapeHtmlAttr(basePath)}">First</a>`);
      parts.push(
        `<a href="${escapeHtmlAttr(`${basePath}?page=${totalPages}`)}">Last (${totalPages})</a>`
      );
      parts.push(`</div>`);
      pagerHtml = parts.join("");
    }

    const bodyHtml = `
<nav class="browse-breadcrumb" aria-label="Breadcrumb"><a href="/">Home</a> / <a href="/employers/">Employers</a> / <span aria-current="page">${escapeHtml(letterHeading(key))}</span></nav>
<h1>${escapeHtml(letterHeading(key))}</h1>
<p class="browse-count">${total.toLocaleString()} employer${total === 1 ? "" : "s"} · <a href="/employers/">All letters</a></p>
${total === 0 ? "<p class=\"lede\">No employers in this group.</p>" : `<ul class="browse-link-list">${items}</ul>`}
${pagerHtml}`;

    const html = browseShell({
      title: `${letterHeading(key)} | H1B Salary Search`,
      description: `Browse ${total.toLocaleString()} H-1B LCA employers ${letterHeading(key).toLowerCase()}. Open salary and job title data per employer.`,
      canonical,
      jsonLd,
      bodyHtml,
    });

    return new Response(html, {
      status: 200,
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": HTML_CACHE,
      },
    });
  } catch (e) {
    console.error("browse letter", e?.message || e);
    return new Response("Server error", { status: 500 });
  }
}

/**
 * @returns {Promise<Response | null>}
 */
export async function trySeoDiscovery(request, env) {
  // Crawlers (e.g. Google) often send HEAD first; only handling GET made sitemaps 404 on HEAD.
  if (request.method !== "GET" && request.method !== "HEAD") return null;
  const url = new URL(request.url);
  let path = url.pathname;
  if (path.length > 1 && path.endsWith("/")) {
    path = path.slice(0, -1);
  }

  if (path === "/sitemap.xml") {
    return serveSitemapIndex(request, env);
  }
  if (path === "/sitemap-static.xml") {
    return serveSitemapStatic(request);
  }

  const chunkMatch = path.match(/^\/sitemap-employers-(\d+)\.xml$/);
  if (chunkMatch) {
    return serveSitemapEmployersChunk(request, env, parseInt(chunkMatch[1], 10));
  }

  const browse = parseBrowseEmployersPath(path);
  if (browse) {
    if (!env.DB) {
      return new Response("Not available", { status: 503 });
    }
    if (browse.type === "hub") {
      return serveBrowseHub(request, env);
    }
    return serveBrowseLetter(request, env, browse.key, url.searchParams);
  }

  return null;
}
