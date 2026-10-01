# Fleetline visual system

Related: [product truth](PRODUCT.md). This is an Operate surface: useful live evidence comes before expression.

The chosen direction is modern transport wayfinding: daylight ivory, restrained pine ink, precise neutral sans typography, information aligned on a readable rail, geography given most of the screen. Seven grounded directions were weighed: transit timetable, logistics manifest, modern atlas, transport signage, contemporary wayfinding, expedition journal, and planning folio. Impeccable's direction roll assigned candidate five. The data-art challenger loses operational clarity; retain its insistence on dense measured information, without theatrical strobing. The monochrome marketing challenger loses task-fit; retain exact hierarchy and one restrained accent. The botanical folio loses rapid-scan identification; retain calm cream grounds and registered map layers. Cassette styling loses both axes; retain bounded list discipline. The orienteering challenger is competitive on geographic clarity but weaker for dashboard operations; retain semantic map legend and actual metric circles. No decorative motif is transplanted.

The first viewport demonstrates live positions, private circular areas and a searchable device list. Empty fleet state gives one concrete next action. Signature interaction: select a device from the map or search to focus it, inspect its precise last report, then follow its motion or propose a geofence. Zones preview spatially before saving. Quietly bounded feeds and 60 rendered list rows carry high-density data without browser churn.

## Tokens

- Family: self-hosted Manrope, medium and bold, licensed SIL Open Font License.
- Type scale: 11/12/13/15/20/27/35 px. Fixed product typography, tabular numeric data.
- Light: ground `#f5f5ef`, surface `#fffefa`, muted `#eeefe8`, ink `#1c3028`, secondary `#626d62`, border `#dedfd5`, action `#255b40`, active soft `#e6eee3`.
- Dark: ground `#18231e`, surface `#202e26`, muted `#28382d`, ink `#edf1e8`, secondary `#b4c0b2`, border `#3c4c40`, action `#afcca0`, active soft `#364b38`.
- Semantic: amber draft `#bb713a`, paused geography `#77836f`, light danger `#ad3c35`, dark danger `#ffa398`.
- Border: 1 px. Controls 6–7 px radius; transient panels 10–12 px; circles only for avatar/map marks.
- Spacing: 6/8/12/16/20/24/28/32 px. Grouped controls tight, surface divisions generous.
- Shadow: `0 8px 18px rgba(30,44,32,.12)`; only elevated panels use deep shadow.
- Motion: 150 ms control state, no staged entrances, reduced motion respected.

Desktop is a fixed operations rail with an expanding map. At widths below 740 px the map leads, then the rail becomes a bounded vertical workspace. Geofence creation is an inline editor, not a modal. Icons share one authored SVG stroke grammar. Keyboard slash opens fleet search, Escape exits the editor. Light/dark selection is persisted. External map tiles are explicitly attributed; fonts and Leaflet ship locally. Every visible statistic is actual fleet, zone or session data.
