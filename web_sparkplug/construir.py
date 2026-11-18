"""Arma panel_web.html (un solo archivo) insertando nucleo.js en la plantilla."""
from pathlib import Path
aqui = Path(__file__).parent
plantilla = (aqui / "panel_web.template.html").read_text(encoding="utf-8")
nucleo = (aqui / "nucleo.js").read_text(encoding="utf-8").replace("</script>", "<\\/script>")
(aqui / "panel_web.html").write_text(plantilla.replace("/*NUCLEO*/", nucleo), encoding="utf-8")
print("panel_web.html generado")
