#!/usr/bin/env python3
"""
Genera los certificados OPC UA (autofirmados) del servidor y del cliente - TP2/TP3 PdCI | UNRaf

  python generar_certificados.py servidor --ip 192.168.10.1      (en la Raspberry, Dispositivo 2)
  python generar_certificados.py cliente  --ip 192.168.10.20     (en la notebook, Dispositivo 3)

Archivos que crea (carpeta --pki, por defecto ./pki):
  servidor  ->  servidor-cert.der  servidor-key.pem
  cliente   ->  cliente-cert.der   cliente-key.pem

Cada certificado lleva el ApplicationUri en el subjectAltName (OPC UA lo compara con el que declara
la aplicacion): servidor = urn:unraf:pdci:t2 | cliente = urn:unraf:pdci:t2:cliente. Si se cambian aca,
hay que cambiarlos igual en servidor_opcua_t3.py (APPLICATION_URI) y gateway_sparkplug.py.

Despues de generarlos hay que intercambiar los certificados PUBLICOS (.der), nunca las claves (.pem):
  cliente-cert.der  ->  Raspberry, carpeta pki/confianza/      (trust list del servidor)
  servidor-cert.der ->  notebook,  carpeta pki/                (certificado del servidor que se verifica)

Requisito: pip install cryptography
"""
import argparse
import ipaddress
import socket
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

APPLICATION_URI = {"servidor": "urn:unraf:pdci:t2", "cliente": "urn:unraf:pdci:t2:cliente"}
NOMBRE = {"servidor": "PdCI T3 - Servidor OPC UA", "cliente": "PdCI T3 - Cliente OPC UA"}


def generar(rol: str, pki: Path, ips, hosts, dias: int, uri: str, forzar: bool):
    cert_path, clave_path = pki / f"{rol}-cert.der", pki / f"{rol}-key.pem"
    if (cert_path.exists() or clave_path.exists()) and not forzar:
        raise SystemExit(f"Ya existen {cert_path} / {clave_path}. Usar --forzar para reemplazarlos "
                         f"(hay que volver a copiar el certificado a la otra maquina).")
    pki.mkdir(parents=True, exist_ok=True)

    clave = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    nombre = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, NOMBRE[rol]),
                        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "UNRaf"),
                        x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, "Protocolos de Comunicaciones Industriales")])
    san = [x509.UniformResourceIdentifier(uri)]
    san += [x509.DNSName(h) for h in hosts]
    san += [x509.IPAddress(ipaddress.ip_address(i)) for i in ips]
    ahora = datetime.now(timezone.utc)
    cert = (x509.CertificateBuilder()
            .subject_name(nombre).issuer_name(nombre)                    # autofirmado
            .public_key(clave.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(ahora - timedelta(minutes=5))              # tolera relojes algo desfasados
            .not_valid_after(ahora + timedelta(days=dias))
            .add_extension(x509.SubjectAlternativeName(san), critical=False)
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.KeyUsage(digital_signature=True, content_commitment=True, key_encipherment=True,
                                         data_encipherment=True, key_agreement=False, key_cert_sign=True,
                                         crl_sign=False, encipher_only=False, decipher_only=False), critical=True)
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH]),
                           critical=False)
            .sign(clave, hashes.SHA256()))

    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.DER))
    clave_path.write_bytes(clave.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                               serialization.NoEncryption()))
    try:
        clave_path.chmod(0o600)                                          # la clave privada solo la lee el dueno
    except OSError:
        pass
    print(f"Certificado: {cert_path}")
    print(f"Clave privada: {clave_path}   (no se comparte ni se sube a ningun lado)")
    print(f"ApplicationUri: {uri}")
    print("subjectAltName: " + ", ".join(["URI:" + uri] + ["DNS:" + h for h in hosts] + ["IP:" + i for i in ips]))
    print(f"Valido hasta: {(ahora + timedelta(days=dias)).date()}")
    if rol == "servidor":
        print("\nSiguiente paso: copiar cliente-cert.der (de la notebook) a", pki / "confianza/")
        print("y copiar servidor-cert.der a la carpeta pki/ de la notebook.")
    else:
        print("\nSiguiente paso: copiar cliente-cert.der a la Raspberry, carpeta pki/confianza/,")
        print("y tener en esta maquina servidor-cert.der (de la Raspberry) en", pki / "servidor-cert.der")


def main():
    ap = argparse.ArgumentParser(description="Genera certificados OPC UA autofirmados")
    ap.add_argument("rol", choices=("servidor", "cliente"))
    ap.add_argument("--ip", action="append", default=[], help="IP de esta maquina (repetible)")
    ap.add_argument("--host", action="append", default=[], help="nombre DNS adicional (repetible); se agrega el hostname")
    ap.add_argument("--dias", type=int, default=1825, help="validez en dias (por defecto 5 anos)")
    ap.add_argument("--pki", default="pki", help="carpeta de salida")
    ap.add_argument("--application-uri", default=None, help="ApplicationUri (por defecto el del rol)")
    ap.add_argument("--forzar", action="store_true", help="reemplaza certificados existentes")
    a = ap.parse_args()
    hosts = list(dict.fromkeys([socket.gethostname()] + a.host))
    try:
        for i in a.ip:
            ipaddress.ip_address(i)
    except ValueError as e:
        sys.exit(f"IP invalida: {e}")
    generar(a.rol, Path(a.pki), a.ip, hosts, a.dias, a.application_uri or APPLICATION_URI[a.rol], a.forzar)


if __name__ == "__main__":
    main()
