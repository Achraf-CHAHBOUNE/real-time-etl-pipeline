from config import DESTINATION_CONFIG  # Reuse from db-extractor
import re

# Pattern to extract Noeud
NOEUD_PATTERN = re.compile(r'^(CALIS|MEIND|RAIND)', re.IGNORECASE)

# KPI formulas for 5min data
KPI_FORMULAS_5MIN = {
    "TxPaging1": {
        "numerator": ["LocNLAPAG1RESUCC", "LocNLAPAG2RESUCC"],
        "denominator": ["LocNLAPAG1TOT"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TxMajLa": {
        "numerator": ["LocNLALOCSUCC"],
        "denominator": ["LocNLALOCTOT"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TxCall_OC": {
        "numerator": ["ChasNCHAFRMSUCC", "ChasNMSFRMSCCI"],
        "denominator": ["ChasNCHAFRMTOT", "ChasNMSFRMTOTI"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TxCall_TC": {
        "numerator": ["ChasNCHATOMSUCC", "ChasNMSTOMSCCO"],
        "denominator": ["ChasNCHATOMTOT", "ChasNMSTOMTOTO"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "EffAuthen_HLR": {
        "numerator": ["SecNAUTFTCSUCC"],
        "denominator": ["SecNAUTFTCTOT"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "Eff_RABASN_In": {
        "numerator": ["RncNRNFRMSCCI"],
        "denominator": ["RncNRNFRMTOTI"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "Eff_RABASN_Out": {
        "numerator": ["RncNRNTOMSCCO"],
        "denominator": ["RncNRNTOMTOTO"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TxHORNCOut": {
        "numerator": ["RncNRNTORGSUCC"],
        "denominator": ["RncNRNTRRRGTOT"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TxHOBSCOut": {
        "numerator": ["BscNBSTOHBSUCC"],
        "denominator": ["BscNBSTRHRTOT"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TxHOBSCIn": {
        "numerator": ["BscNBSTIHBSUCC", "BscNBSTIUGHBSUCC"],
        "denominator": ["BscNBSTSHRTOT", "BscNBSTSUGHRTOT"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TxSms_MO": {
        "numerator": ["ShmNSMSCAOSUCC"],
        "denominator": ["ShmNSMSRDOTOT"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TxSms_MT": {
        "numerator": ["ShmNSMSSRSUCC"],
        "denominator": ["ShmNSMSSMRLTOT"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TRAF_Erlang_S": {
        "numerator": ["TrunkrouteNTRALACCO"],
        "denominator": ["TrunkrouteNSCAN"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) if sum(denom) != 0 else None
    },
    "TRAF_Erlang_E": {
        "numerator": ["TrunkrouteNTRALACCI"],
        "denominator": ["TrunkrouteNSCAN"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) if sum(denom) != 0 else None
    },
    "TRAF_RDT": {
        "numerator": ["TrunkrouteNTRALACCO", "TrunkrouteNTRALACCI"],
        "denominator": ["TrunkrouteNSCAN"],
        "additional": ["TrunkrouteNDEV", "TrunkrouteNBLOCACC"],
        "formula": lambda num, denom, add: ((sum(num) / sum(denom)) / (add[0] - (add[1] / sum(denom)))) * 100 if (sum(denom) != 0 and (add[0] - (add[1] / sum(denom))) != 0) else None
    },
    "TRAF_CircHS": {
        "numerator": ["TrunkrouteNBLOCACC"],
        "denominator": ["TrunkrouteNSCAN", "TrunkrouteNDEV"],
        "formula": lambda num, denom: (sum(num) / denom[0]) / denom[1] * 100 if (denom[0] != 0 and denom[1] != 0) else None
    },
    "TRAF_ALOC_E": {
        "numerator": ["TrunkrouteNTRALACCI"],
        "denominator": ["TrunkrouteNSCAN", "TrunkrouteNANSWERSI"],
        "formula": lambda num, denom: (sum(num) / denom[0]) / denom[1] * 300 if (denom[0] != 0 and denom[1] != 0) else None
    },
    "TRAF_ALOC_S": {
        "numerator": ["TrunkrouteNTRALACCO"],
        "denominator": ["TrunkrouteNSCAN", "TrunkrouteNANSWERSO"],
        "formula": lambda num, denom: (sum(num) / denom[0]) / denom[1] * 300 if (denom[0] != 0 and denom[1] != 0) else None
    },
    "ASR_S": {
        "numerator": ["TrunkrouteNANSWERSO"],
        "denominator": ["TrunkrouteNCALLSO", "TrunkrouteNOVERFLOWO"],
        "formula": lambda num, denom: (sum(num) / (denom[0] - denom[1])) * 100 if (denom[0] - denom[1]) != 0 else None
    },
    "ASR_E": {
        "numerator": ["TrunkrouteNANSWERSI"],
        "denominator": ["TrunkrouteNCALLSI"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TRAF_FCS": {
        "numerator": ["TrunkrouteNBLOCACC"],
        "denominator": ["TrunkrouteNSCAN"],
        "additional": ["TrunkrouteNDEV"],
        "formula": lambda num, denom, add: add[0] - (sum(num) / sum(denom)) if sum(denom) != 0 else None
    },
    "RouteUtilizationIn": {
        "numerator": ["VoiproITRALAC"],
        "denominator": ["VoiproNTRAFIND STASIPI"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "RouteUtilizationOut": {
        "numerator": ["VoiproOTRALAC"],
        "denominator": ["VoiproNTRAFIND STASIPO"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "Succ_VoIP_Seiz_Attempts": {
        "numerator": ["VoiproIOVERFL"],
        "formula": lambda num: (1 - sum(num)) * 100
    },
    "ASR_IN": {
        "numerator": ["VoiproIANSWER"],
        "denominator": ["VoiproNCALLSI"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "ASR_OUT": {
        "numerator": ["VoiproOANSWER"],
        "denominator": ["VoiproNCALLSO"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "Success_SIP_IN": {
        "numerator": ["SiproISUCSES"],
        "denominator": ["SiproISIPSES"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "Success_SIP_OUT": {
        "numerator": ["SiproOSUCSES"],
        "denominator": ["SiproOSIPSES"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "Invite_Req_Succ_Ratio": {
        "numerator": ["SipnodNUSINVITE"],
        "denominator": ["SipnodNRINVITE"],
        "formula": lambda num, denom: (1 - (sum(num) / sum(denom))) * 100 if sum(denom) != 0 else None
    },
    "Rec_SIP_Req_Succ_Ratio": {
        "numerator": ["SipnodONSIPRES"],
        "denominator": ["SipnodINSIPREQ"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "Sent_SIP_Req_Succ_Ratio": {
        "numerator": ["SipnodINSIPRES"],
        "denominator": ["SipnodONSIPREQ"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "ALOC_IN": {
        "numerator": ["VoiproITRALAC"],
        "denominator": ["VoiproNSCAN", "VoiproIANSWER"],
        "formula": lambda num, denom: (sum(num) / denom[0]) / denom[1] * 300 if (denom[0] != 0 and denom[1] != 0) else None
    },
    "ALOC_OUT": {
        "numerator": ["VoiproOTRALAC"],
        "denominator": ["VoiproNSCAN", "VoiproOANSWER"],
        "formula": lambda num, denom: (sum(num) / denom[0]) / denom[1] * 300 if (denom[0] != 0 and denom[1] != 0) else None
    },
    "CSFB_MT_Eff": {
        "numerator": ["CsfbNSUCCCSFB"],
        "denominator": ["CsfbNSPAG1CSFB", "CsfbNSPAG2CSFB"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "CSFB_Call_MT": {
        "numerator": ["CsfbNSUCCCSFB"],
        "denominator": ["CsfbNSUCCCSFB", "CsfbNUNSUCCCSFB", "CsfbNUSREJCSFB"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "CSFB_Paging": {
        "numerator": ["CsfbNSPAG1CSFB", "CsfbNSPAG2CSFB"],
        "denominator": ["CsfbNTPAG1CSFB"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "SGS_UpdateLocation": {
        "numerator": ["SgsNSLOCREGSGS"],
        "denominator": ["SgsNTLOCREGSGS"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "SGS_SMS_MO": {
        "numerator": ["SgsNSMOSMS"],
        "denominator": ["SgsNTMOSMS"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "SGS_SMS_MT": {
        "numerator": ["SgsNSMTSMS"],
        "denominator": ["SgsNTMTSMS"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "SGSLA_Attach_Reg": {
        "numerator": ["SgslaNSLAATREGSGS"],
        "denominator": ["SgslaNTLAATREGSGS"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "SGSLA_Attach_NonReg": {
        "numerator": ["SgslaNSLAATNREGSGS"],
        "denominator": ["SgslaNTLAATNREGSGS"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "SGSLA_LocUpdate_Reg": {
        "numerator": ["SgslaNSLANLREGSGS"],
        "denominator": ["SgslaNTLANLREGSGS"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "SGSLA_LocUpdate_NonReg": {
        "numerator": ["SgslaNSLANLNREGSGS"],
        "denominator": ["SgslaNTLANLNREGSGS"],
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    }
}