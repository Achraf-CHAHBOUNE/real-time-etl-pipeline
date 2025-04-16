import re
from dotenv import load_dotenv
from typing import Dict
import os

# Pattern to extract Node
NOEUD_PATTERN_5_15 = re.compile(r'^(CALIS|MEIND|RAIND)', re.IGNORECASE)

# Database connection parameters
load_dotenv()

# Source Database connection parameters
SOURCE_DB_HOST = os.getenv("SOURCE_MYSQL_HOST")
SOURCE_DB_USER = os.getenv("SOURCE_MYSQL_USER")
SOURCE_DB_PASSWORD = os.getenv("SOURCE_MYSQL_PASSWORD")
SOURCE_DB_NAME = os.getenv("SOURCE_MYSQL_DB")
SOURCE_DB_PORT = int(os.getenv("SOURCE_MYSQL_PORT", default=3306))

# Destination Database connection parameters
DEST_DB_HOST = os.getenv("DEST_MYSQL_HOST")
DEST_DB_USER = os.getenv("DEST_MYSQL_USER")
DEST_DB_PASSWORD = os.getenv("DEST_MYSQL_PASSWORD")
DEST_DB_NAME = "5min_transform"
DEST_DB_PORT = int(os.getenv("DEST_MYSQL_PORT", default=3306))

# Source Database config
SOURCE_DB_CONFIG = {
    'host': SOURCE_DB_HOST,
    'user': SOURCE_DB_USER,
    'password': SOURCE_DB_PASSWORD,
    'port': 3307,
    'database': SOURCE_DB_NAME
}

# Destination Database config
DEST_DB_CONFIG = {
    'host': DEST_DB_HOST,
    'user': DEST_DB_USER,
    'password': DEST_DB_PASSWORD,
    'port': 3307,
    'database': DEST_DB_NAME
}

# Files config
files_paths: Dict[str, str] = {
    '5min': './data/our_data/result_5min.txt',
    '15min': './data/our_data/result_15min.txt',
    'mgw': './data/our_data/result_mgw.txt',
    'last_extracted': './data/last_extracted.json'
}

# Suffix to operator/network mapping (lowercase keys)
SUFFIX_OPERATOR_MAPPING = {
    'nw': 'Inwi',
    'mt': 'Maroc Telecom',
    'ie': 'International',
    'is': 'International',
    'bs': 'Orange 2G',
    'be': 'Orange 2G',
    'ne': 'Orange 3G',
    'ns': 'Orange 3G'
}

# KPI formulas for 5min data (spaces replaced with underscores)
KPI_FORMULAS_5MIN = {
    "TxPaging1": {
        "numerator": ["LocNLAPAG1RESUCC", "LocNLAPAG2RESUCC"],
        "denominator": ["LocNLAPAG1LOTOT"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TxMajLa": {
        "numerator": ["LocNLALOCSUCC"],
        "denominator": ["LocNLALOCTOT"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TxCall_OC": {
        "numerator": ["ChasNCHAFRMSUCC", "ChasNMSFRMSCCI"],
        "denominator": ["ChasNCHAFRMTOT", "ChasNMSFRMTOTI"],
        "Suffix": False,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TxCall_TC": {
        "numerator": ["ChasNCHATOMSUCC", "ChasNMSTOMSCCO"],
        "denominator": ["ChasNCHATOMTOT", "ChasNMSTOMTOTO"],
        "Suffix": False,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "EffAuthen_HLR": {
        "numerator": ["SecNAUTFTCSUCC"],
        "denominator": ["SecNAUTFTCTOT"],
        "Suffix": False,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "Eff_RABASN_In": {
        "numerator": ["RncNRNFRMSCCI"],
        "denominator": ["RncNRNFRMTOTI"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "Eff_RABASN_Out": {
        "numerator": ["RncNRNTOMSCCO"],
        "denominator": ["RncNRNTOMTOTO"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TxHORNCOut": {
        "numerator": ["RncNRNTORGSUCC"],
        "denominator": ["RncNRNTRRRGTOT"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TxHOBSCOut": {
        "numerator": ["BscNBSTOHBSUCC"],
        "denominator": ["BscNBSTRHRTOT"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TxHOBSCIn": {
        "numerator": ["BscNBSTIHBSUCC", "BscNBSTIUGHBSUCC"],
        "denominator": ["BscNBSTSHRTOT", "BscNBSTSUGHRTOT"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TxSms_MO": {
        "numerator": ["ShmNSMSCAOSUCC"],
        "denominator": ["ShmNSMSRDOTOT"],
        "Suffix": False,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TxSms_MT": {
        "numerator": ["ShmNSMSSRSUCC"],
        "denominator": ["ShmNSMSSMRLTOT"],
        "Suffix": False,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TRAF_Erlang_S": {
        "numerator": ["TrunkrouteNTRALACCO"],
        "denominator": ["TrunkrouteNSCAN"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) if sum(denom) != 0 else None
    },
    "TRAF_Erlang_E": {
        "numerator": ["TrunkrouteNTRALACCI"],
        "denominator": ["TrunkrouteNSCAN"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) if sum(denom) != 0 else None
    },
    "TRAF_RDT": {
        "numerator": ["TrunkrouteNTRALACCO", "TrunkrouteNTRALACCI"],
        "denominator": ["TrunkrouteNSCAN"],
        "additional": ["TrunkrouteNDEV", "TrunkrouteNBLOCACC"],
        "Suffix": True,
        "formula": lambda num, denom, add: ((sum(num) / sum(denom)) / (add[0] - (add[1] / sum(denom)))) * 100 if (sum(denom) != 0 and (add[0] - (add[1] / sum(denom))) != 0) else None
    },
    "TRAF_CircHS": {
        "numerator": ["TrunkrouteNBLOCACC"],
        "denominator": ["TrunkrouteNSCAN", "TrunkrouteNDEV"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / denom[0]) / denom[1] * 100 if (denom[0] != 0 and denom[1] != 0) else None
    },
    "TRAF_ALOC_E": {
        "numerator": ["TrunkrouteNTRALACCI"],
        "denominator": ["TrunkrouteNSCAN", "TrunkrouteNANSWERSI"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / denom[0]) / denom[1] * 300 if (denom[0] != 0 and denom[1] != 0) else None
    },
    "TRAF_ALOC_S": {
        "numerator": ["TrunkrouteNTRALACCO"],
        "denominator": ["TrunkrouteNSCAN", "TrunkrouteNANSWERSO"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / denom[0]) / denom[1] * 300 if (denom[0] != 0 and denom[1] != 0) else None
    },
    "ASR_S": {
        "numerator": ["TrunkrouteNANSWERSO"],
        "denominator": ["TrunkrouteNCALLSO", "TrunkrouteNOVERFLOWO"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / (denom[0] - denom[1])) * 100 if (denom[0] - denom[1]) != 0 else None
    },
    "ASR_E": {
        "numerator": ["TrunkrouteNANSWERSI"],
        "denominator": ["TrunkrouteNCALLSI"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TRAF_FCS": {
        "numerator": ["TrunkrouteNBLOCACC"],
        "denominator": ["TrunkrouteNSCAN"],
        "additional": ["TrunkrouteNDEV"],
        "Suffix": True,
        "formula": lambda num, denom, add: add[0] - (sum(num) / sum(denom)) if sum(denom) != 0 else None
    },
    "RouteUtilizationIn": {
        "numerator": ["VoiproITRALAC"],
        "denominator": ["VoiproNTRAFIND_STASIPI"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "RouteUtilizationOut": {
        "numerator": ["VoiproOTRALAC"],
        "denominator": ["VoiproNTRAFIND_STASIPO"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "Succ_VoIP_Seiz_Attempts": {
        "numerator": ["VoiproIOVERFL"],
        "Suffix": True,
        "formula": lambda num: (1 - sum(num)) * 100
    },
    "ASR_IN": {
        "numerator": ["VoiproIANSWER"],
        "denominator": ["VoiproNCALLSI"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "ASR_OUT": {
        "numerator": ["VoiproOANSWER"],
        "denominator": ["VoiproNCALLSO"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "Success_SIP_IN": {
        "numerator": ["SiproISUCSES"],
        "denominator": ["SiproISIPSES"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "Success_SIP_OUT": {
        "numerator": ["SiproOSUCSES"],
        "denominator": ["SiproOSIPSES"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "Invite_Req_Succ_Ratio": {
        "numerator": ["SipnodNUSINVITE"],
        "denominator": ["SipnodNRINVITE"],
        "Suffix": False,
        "formula": lambda num, denom: (1 - (sum(num) / sum(denom))) * 100 if sum(denom) != 0 else None
    },
    "Rec_SIP_Req_Succ_Ratio": {
        "numerator": ["SipnodONSIPRES"],
        "denominator": ["SipnodINSIPREQ"],
        "Suffix": False,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "Sent_SIP_Req_Succ_Ratio": {
        "numerator": ["SipnodINSIPRES"],
        "denominator": ["SipnodONSIPREQ"],
        "Suffix": False,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "ALOC_IN": {
        "numerator": ["VoiproITRALAC"],
        "denominator": ["VoiproNSCAN", "VoiproIANSWER"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / denom[0]) / denom[1] * 300 if (denom[0] != 0 and denom[1] != 0) else None
    },
    "ALOC_OUT": {
        "numerator": ["VoiproOTRALAC"],
        "denominator": ["VoiproNSCAN", "VoiproOANSWER"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / denom[0]) / denom[1] * 300 if (denom[0] != 0 and denom[1] != 0) else None
    },
    "CSFB_MT_Eff": {
        "numerator": ["CsfbNSUCCCSFB"],
        "denominator": ["CsfbNSPAG1CSFB", "CsfbNSPAG2CSFB"],
        "Suffix": False,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "CSFB_Call_MT": {
        "numerator": ["CsfbNSUCCCSFB"],
        "denominator": ["CsfbNSUCCCSFB", "CsfbNUNSUCCCSFB", "CsfbNUSREJCSFB"],
        "Suffix": False,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "CSFB_Paging": {
        "numerator": ["CsfbNSPAG1CSFB", "CsfbNSPAG2CSFB"],
        "denominator": ["CsfbNTPAG1CSFB"],
        "Suffix": False,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "SGS_UpdateLocation": {
        "numerator": ["SgsNSLOCREGSGS"],
        "denominator": ["SgsNTLOCREGSGS"],
        "Suffix": False,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "SGS_SMS_MO": {
        "numerator": ["SgsNSMOSMS"],
        "denominator": ["SgsNTMOSMS"],
        "Suffix": False,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "SGS_SMS_MT": {
        "numerator": ["SgsNSMTSMS"],
        "denominator": ["SgsNTMTSMS"],
        "Suffix": False,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "SGSLA_Attach_Reg": {
        "numerator": ["SgslaNSLAATREGSGS"],
        "denominator": ["SgslaNTLAATREGSGS"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "SGSLA_Attach_NonReg": {
        "numerator": ["SgslaNSLAATNREGSGS"],
        "denominator": ["SgslaNTLAATNREGSGS"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "SGSLA_LocUpdate_Reg": {
        "numerator": ["SgslaNSLANLREGSGS"],
        "denominator": ["SgslaNTLANLREGSGS"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "SGSLA_LocUpdate_NonReg": {
        "numerator": ["SgslaNSLANLNREGSGS"],
        "denominator": ["SgslaNTLANLNREGSGS"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    }
}