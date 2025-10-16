def generar_prompt_iva(rut, compras, ventas, pregunta_usuario):
    texto_compras = formatear(compras, "compras")
    texto_ventas = formatear(ventas, "ventas")

    return f"""
Contexto tributario para RUT {rut} (últimos meses disponibles):

**Resumen Compras:**
{texto_compras}

**Resumen Ventas:**
{texto_ventas}

Considerando la información anterior, responde la siguiente pregunta como un asesor tributario experto:

Pregunta: {pregunta_usuario}
Respuesta:"""

def formatear(data, tipo):
    """
    Formatea la lista de datos mensuales (compras o ventas) para incluirla en el prompt.
    """
    if not data:
        return f"No se encontraron datos de {tipo} recientes."

    lineas = []
    # Iterar sobre la lista de diccionarios mensuales
    for month_data in data:
        periodo = month_data.get("period", "N/A")
        if tipo == "compras":
            total = month_data.get("total_purchases", 0)
            iva = month_data.get("total_purchases_iva", 0)
            lineas.append(f"- Periodo {periodo}: Total ${total:,.0f}, IVA ${iva:,.0f}")
        elif tipo == "ventas":
            total = month_data.get("total_sales", 0)
            iva = month_data.get("total_sales_iva", 0)
            lineas.append(f"- Periodo {periodo}: Total ${total:,.0f}, IVA ${iva:,.0f}")

    return "\n".join(lineas)

# Código anterior comentado para referencia
# def formatear(data, tipo):
#     if not data or "periodos" not in data:
#         return f"No se encontraron {tipo} recientes."
#
#     linea = f"{tipo.capitalize()}:\n"
#     for p in data["periodos"]:
#         linea += f"- {p['periodo']}: ${p['monto']:,}\n"
#     return linea