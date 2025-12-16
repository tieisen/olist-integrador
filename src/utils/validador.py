import re

class Validador:

    def __init__(self):
        pass

    def gtin(gtin: str) -> bool:
        # Remove espaços e valida se é numérico
        gtin = gtin.strip()
        if not gtin.isdigit():
            return False

        # GTINs válidos: 8, 12, 13 ou 14 dígitos
        if len(gtin) not in (8, 12, 13, 14):
            return False

        numeros = [int(d) for d in gtin]
        digito_verificador = numeros.pop()

        soma = 0
        # percorre da direita para a esquerda
        for i, n in enumerate(reversed(numeros)):
            soma += n * 3 if i % 2 == 0 else n

        dv_calculado = (10 - (soma % 10)) % 10
        return dv_calculado == digito_verificador
    
    def ncm(self,ncm:str) -> str:
        """
        Adiciona máscara no NCM
            :param ncm: código do NCM
            :return str: código do NCM com máscara
        """
        if not ncm:
            return None
        ncm = re.sub(r"(\d{4})(\d{2})(\d{2})", r"\1.\2.\3", str(ncm))
        if len(ncm) != 10:
            return None
        return ncm

    def cest(self,cest:str) -> str:
        """
        Adiciona máscara no CEST
            :param cest: código do CEST
            :return str: código do CEST com máscara
        """        
        if not cest:
            return None
        cest = re.sub(r"(\d{2})(\d{3})(\d{2})", r"\1.\2.\3", str(cest))
        if len(cest) != 9:
            return None
        return cest    