from functionWrappers import *
from classes import *

######################################### 1st Iteration ###########################################


######################################### last name ###########################################
def lastName():
    return node(ifW, [node(andW, [node(isNotEmptyW, [paramnode(0), paramnode(1)]),
                               node(isNotEmptyW, [paramnode(0), paramnode(3)])]),
                   node(compareStringW, [node(getValueW, [paramnode(0), paramnode(1)]),
                                         node(getValueW, [paramnode(0), paramnode(3)])]),
                   node(handleErrorW,[constnode("R"), paramnode(2), node(getValueW, [paramnode(0), paramnode(1)])])
                   ])



####################################### TAX ID ################################################
def taxId():
    return node(ifW, [
        node(isNotEmptyW, [paramnode(0), paramnode(1)]),
        node(getValueW, [paramnode(0), paramnode(1)]),
        node(handleErrorW, [constnode("C"), paramnode(2), node(getValueW, [paramnode(0), paramnode(1)])])
            ])



################################ NPI ###########################################################
def npi():
    return node(ifW, [
        node(isNotEmptyW, [paramnode(0), paramnode(1)]),
        node(getValueW, [paramnode(0), paramnode(1)]),
        node(handleErrorW,[constnode("C"), paramnode(2), node(getValueW, [paramnode(0), paramnode(1)])])
                ])



############################# First name #######################################################
def firstName():
    return node(ifW, [
        node(andW, [node(isNotEmptyW, [paramnode(0), paramnode(1)]),
                    node(isNotEmptyW, [paramnode(0), paramnode(2)])
                    ]),
        node(compareStringW, [
            node(getValueW, [paramnode(0), paramnode(1)]),
            node(getValueW, [paramnode(0), paramnode(2)])
        ]),
        node(ifW, [
            node(isEqualW, [
                node(getValueW, [paramnode(0), paramnode(3)]),
                constnode("G")
            ]),
            node(rejectW, [paramnode(4)]),
            constnode("G")
        ])

    ])


######################################## Middle Name ###############################################
def middleName():
    return node(ifW, [node(isNotEmptyW, [paramnode(0),paramnode(1)]),
                      node(getValueW, [paramnode(0),paramnode(2)]),
                      constnode("")
                                          ])



######################################## Alternate_F_Name ###############################################
def alternate_f_name():
    return node(ifW, [
        node(isNotEmptyW, [paramnode(0),paramnode(1)]),
        node(getValueW, [paramnode(0),paramnode(1)]),
        constnode("")
    ])


######################################## DOB ###################################################
def dob():
    return node(ifW, [
                        node(isEqualW, [
                            node(getValueW, [paramnode(0),paramnode(1)]),
                            constnode("I")]),
                        node(ifW, [
                            node(isNotEmptyW, [paramnode(0),paramnode(2)]),
                            node(getValueW, [paramnode(0),paramnode(2)]),
                            constnode("")
                        ]),
                        constnode("")
    ])



# ######################################## GENDER ###################################################
# def gender():
#     return node(ifW, [node(isNotEmptyW, [paramnode(0), paramnode(1)]),
#                       node(getValueW, [paramnode(0), paramnode(1)]),
#                       node(getValueW, [paramnode(0), paramnode(2)])
#                       ])
#     # return node(getValueW, [paramnode(0), paramnode(1)])


#ssn
def ssn():
    return node(ifW, [
        node(isEqualW, [
                    node(getValueW, [paramnode(0),paramnode(1)]),
                    constnode("I")
        ]),
        node(ifW, [
                    node(isNotEmptyW, [paramnode(0),paramnode(2)]),
                    node(getValueW, [paramnode(0),paramnode(2)]),
                    constnode("")
        ]),
        constnode("")
    ])




######################################## MediCare ###################################################
def mediCare():
    return  node(ifW, [
                node(isNotEmptyW, [paramnode(0),paramnode(1)]),
                node(getValueW, [paramnode(0),paramnode(1)]),
                constnode("")
                ])

######################################### 2nd Iteration ###########################################

######################################## ALT_L_NAME  ###################################################
def alt_l_name():
    return node(ifW, [
           node(isEqualW, [
                node(getValueW, [paramnode(0),paramnode(1)]),
                constnode("G")
           ]),
           constnode(""),
           node(ifW, [
               node(isNotEmptyW, [paramnode(0),paramnode(2)]),
               node(getValueW, [paramnode(0),paramnode(2)]),
               constnode("")
                ])
            ])
####################################### BOARD_CERT_DATE###################################################
def board_cert_date():
    return node(ifW, [node(isNotEmptyW, [paramnode(0),paramnode(3)]),
                      node(ifW, [
                          node(isEqualIntW, [
                              node(getValueW, [paramnode(0), paramnode(1)]),
                              constnode("1"),
                          ]),
                          node(getValueW, [paramnode(0), paramnode(3)]),
                          constnode("")
                      ]),
                      constnode("")
                ])

# ######################################## BOARD_CERT_EXPIRATION_DATE###################################################
def board_expiration_date():
    return node(ifW, [node(isNotEmptyW, [paramnode(0), paramnode(3)]),
                      node(ifW, [
                          node(isEqualIntW, [
                              node(getValueW, [paramnode(0), paramnode(1)]),
                              constnode("1"),
                          ]),
                          node(getValueW, [paramnode(0), paramnode(3)]),
                          constnode("")
                      ]),
                      constnode("")
                      ])
# ########################################LICENCE_NUMBER###################################################
def licence_number():
    return node(ifW, [
        node(isNotEmptyW, [paramnode(0),paramnode(1)]),
        node(getValueW, [paramnode(0),paramnode(1)]),
        constnode("")
    ])
# ########################################LICENCE_STATE###################################################
def license_state():
    return node(ifW, [
        node(isNotEmptyW, [paramnode(0),paramnode(1)]),
        node(ifW, [
            node(isNotEmptyW, [paramnode(0),paramnode(2)]),
            node(getValueW, [paramnode(0),paramnode(2)]),
            constnode("")
        ]),
        constnode("")
    ])

# ########################################STATE_LICENCE_EFFECTIVE_DATE###################################################
def state_license_effective_date():
    return node(ifW, [
        node(isNotEmptyW,[paramnode(0),paramnode(1)]),
        node(ifW,[
                    node(isNotEmptyW, [paramnode(0),paramnode(2)]),
                    node(getValueW, [paramnode(0),paramnode(2)]),
                    node(rejectW, [paramnode(3)])
        ]),
        constnode("")
    ])
# ########################################License Expiry Date(###################################################
def licenseExpiryDate():
    return node(ifW, [node(isNotEmptyW, [paramnode(0), paramnode(1)]),
                      node(ifW, [node(isNotEmptyW, [paramnode(0), paramnode(2)]), node(getValueW, [paramnode(0), paramnode(2)]), node(rejectW, [paramnode(3)])]),
                      constnode("")
                ])
# ####################################### cdsCsrNumber ################################################
def cdsCsrNumber():
    return node(ifW, [
                    node(isNotEmptyW, [paramnode(0), paramnode(1)]),
                    node(getValueW, [paramnode(0), paramnode(1)]),
                    constnode("")])
# ####################################### cdsState ################################################
def cdsState():
    return node(ifW, [
                    node(isNotEmptyW, [paramnode(0), paramnode(1)]),
                    node(ifW, [
                             node(isNotEmptyW, [paramnode(0), paramnode(2)]),
                             node(getValueW, [paramnode(0), paramnode(2)]),
                             constnode("")]),
                    constnode("")])
# ####################################### cdsExpiryDate ################################################
#
def cdsExpiryDate():
    return node(ifW, [
                      node(isNotEmptyW, [paramnode(0), paramnode(1)]),
                      node(ifW, [node(isNotEmptyW, [paramnode(0), paramnode(2)]),
                                 node(getValueW, [paramnode(0), paramnode(2)]),
                                 constnode("")]),
                      constnode("")])

#
#
# ####################################### enumerationDate ################################################
def enumerationDate():
    return node(getValueW, [paramnode(0), paramnode(1)])
#
# ####################################### originalDelDate ################################################
def originalDelDate():
    return node(ifW, [
                    node(isNotEmptyW, [paramnode(0), paramnode(1)]),
                    node(getValueW, [paramnode(0), paramnode(1)]),
                    node(rejectW, [paramnode(2)])])

#########################################EffectiveDate###############################################
def effectiveDate():
    return node(ifW, [node(isNotEmptyW, [paramnode(0),paramnode(1)]),
                      node(ifW, [node(isGreaterThenW, [
                                                        node(getDiffRealW, [node(getValueW, [paramnode(0),paramnode(2)]),
                                                                        node(getValueW, [paramnode(0),paramnode(1)])
                                                                        ]),
                                                        constnode(730)
                                                        ]),
                                 node(handleErrorW,[constnode("R"), paramnode(3), node(getValueW, [paramnode(0), paramnode(1)])]),
                                 node(ifW, [node(isGreaterThenW, [
                                                        node(getDiffRealW, [node(getValueW, [paramnode(0),paramnode(2)]),
                                                                        node(getValueW, [paramnode(0),paramnode(1)])
                                                                        ]),
                                                        constnode(365)
                                                        ]),
                                            node(handleErrorW, [constnode("C"), paramnode(4),node(getValueW, [paramnode(0), paramnode(1)])]),
                                            node(ifW, [node(isGreaterThenW, [
                                                        node(getDiffRealW, [
                                                            node(getValueW, [paramnode(0),paramnode(1)]),
                                                            node(getValueW, [paramnode(0),paramnode(2)])
                                                                        ]),
                                                        constnode(90)
                                                        ]),
                                                       node(handleErrorW,[constnode("R"), paramnode(5),node(getValueW, [paramnode(0), paramnode(1)])]),
                                                       node(getValueW, [paramnode(0),paramnode(1)])
                                                       ])
                                            ])

                                 ]),
                      constnode("")
                      ])

# #########################################EffectiveDate2###############################################
def effectiveDate2():
    return node(ifW, [node(isNotEmptyW, [paramnode(0),paramnode(1)]),
                     node(getValueW, [paramnode(0),paramnode(1)]),
                     node(ifW, [node(isNotEmptyW, [paramnode(0),paramnode(2)]),
                                node(getValueW, [paramnode(0),paramnode(2)]),
                                node(ifW, [node(getValueW, [paramnode(0),paramnode(3)]),
                                           node(handleErrorW, [constnode("C"), paramnode(4),node(getValueW, [paramnode(0), paramnode(1)])]),
                                          constnode("")
                                           ])
                                ])
                ])

def primarySpecialityOrSpecialityCode():
    return node(ifW, [node(isNotEmptyW, [paramnode(0),paramnode(1)]),
                      node(getValueW, [paramnode(0),paramnode(1)]),
                      constnode("")
                ])

# def degreeCredentialsTitle():
#     return node(ifW, [node(isNotEmptyW, [paramnode(0),paramnode(1)]),
#                       node(ifW, [listContainsW, node(getDistinctW, [paramnode(0),paramnode(1)]),
#                                                 node(getDictValueW, [paramnode(2),paramnode(3)])
#                                        ]),
#                             node(getDistinctW, [paramnode(0),paramnode(1)]),
#                             node(getValueW, [paramnode(0),paramnode(4)]),
#                       node(getValueW, [paramnode(0),paramnode(4)])
#                       ])
def isCurrentDelDateNotEmpty():
    return node(ifW, [
        node(isNotEmptyW, [paramnode(0), paramnode(1)]),
        constnode("Y"),
        constnode("N")
    ])

def isOrigDelDateNotEmpty():
    return node(ifW, [
        node(isNotEmptyW, [paramnode(0), paramnode(1)]),
        constnode("Y"),
        constnode("N")
    ])

def isEffectiveDateNotEmpty():
    return node(ifW, [
        node(isNotEmptyW, [paramnode(0), paramnode(1)]),
        constnode("Y"),
        constnode("N")
    ])

def credDateGreater1day():
    return node(ifW, [
        node(isNotEmptyW, [paramnode(0),paramnode(1)]),
        node(ifW, [
            node(isGreaterThenW, [node(getDiffRealW, [node(getValueW, [paramnode(0), paramnode(1)]),
                                                  node(getValueW, [paramnode(0), paramnode(2)])
                                                  ]),
                                  constnode("1")
                                  ]),
            constnode("Y"),
            constnode("N")
        ]),
        constnode("N")
    ])

def CurDelGreater2days():
    return node(ifW, [
        node(isNotEmptyW, [paramnode(0),paramnode(1)]),
        node(ifW, [
            node(isGreaterThenW, [node(getDiffW, [node(getValueW, [paramnode(0), paramnode(1)]),
                                                  node(getValueW, [paramnode(0), paramnode(2)])
                                                  ]),
                                  constnode("2")
                                  ]),
            constnode("Y"),
            constnode("N")
        ]),
        constnode("N")
    ])

# def CurDelGreaterOrigDate():
#     node(ifW, [
#         node(isGreaterDateW, [
#                 node(getValueW, [paramnode(0), paramnode(1)]),
#                 node(getValueW, [paramnode(0), paramnode(2)])
#             ]),
#         constnode("Y"),
#         constnode("N")
#     ])

def currentDelDate():
    return node(ifW, [node(isNotEmptyW, [paramnode(0),paramnode(1)]),
                      node(ifW, [node(isEqualW, [node(getValueW, [paramnode(0),paramnode(2)]),
                                                       constnode("Y")
                                                       ]),
                                 node(getValueW, [paramnode(0),paramnode(3)]),
                                 node(ifW, [node(isEqualW, [node(getValueW, [paramnode(0),paramnode(2)]),
                                                       constnode("Y")
                                                       ]),
                                            node(getValueW, [paramnode(0),paramnode(3)]),
                                            node(getValueW, [paramnode(0), paramnode(1)])
                                            ])
                                 ]),
                      node(ifW, [node(getValueW, [paramnode(0),paramnode(4)]),
                                 constnode("Clarification for Cred Date"),
                                 node(getValueW, [paramnode(0),paramnode(5)])
                                 ])
                      ]
                )
#
# def taxonomy():
#     return node(ifW, [
#                         node(isNotEmptyW, [paramnode[0], paramnode[1]]),
#                         node(getValueW,[paramnode(0), paramnode(1)]),
#                         node(getValueW, [paramnode(0), paramnode(1)]),
#                       ])

def electronic_comm1():
    return node(ifW, [node(isNotEmptyW, [paramnode(0), paramnode(1)]),
               node(getValueW, [paramnode(0), paramnode(1)]),
               constnode("")
               ])


def electronic_comm2():
    return node(ifW, [node(isNotEmptyW, [paramnode(0), paramnode(1)]),
               node(getValueW, [paramnode(0), paramnode(1)]),
               constnode("")
               ])
#####################################Board Certified Date##########################################
def var_board1():
    return node(ifW, [node(listContainsW, [constnode(["DO","MD","PHD","DPM"]),
                                           paramnode(1)]),
                      constnode("1"),
                      constnode("0")
                      ])

def var_board2():
    return node(ifW, [node(checkFormatW, [node(getValueW, [paramnode(0),paramnode(1)]),
                                          constnode("12/31/2999")
                                          ]),
                      constnode("1"),
                      constnode("0")
                      ])
def var_board3():
    return node(ifW, [node(isNotEmptyW, [paramnode(1),paramnode(2)]),
                      constnode("1"),
                      constnode("0")
                      ])
def var_board4():
    return node(ifW, [node(listContainsW, [paramnode(0),constnode(["Y","N"])]),
                      constnode("1"),
                      constnode("0")
                      ])
def var_board5():
    return node(ifW, [node(isNotEmptyW, [paramnode(0),paramnode(1)]),
                      constnode("1"),
                      constnode("0")
                      ])
# def var_board6():{"name": "var_board6", "type": "tree", "input": ["masterDict['singleValue']['Board_Certified']", "'VAR_BOARD4'", "'VAR_BOARD5'","'VAR_BOARD3'","'VAR_BOARD2'"], "col": "VAR_BOARD6"},
#     return node(ifW, [node(isNotEmptyW, [paramnode(0),paramnode(1)]),
#                         node(ifW, [node(getValueW, [paramnode(0),paramnode(2)]),
#                                    node(ifW, [node(andW, [node(isEqualW, [
#                                                             node(getValueW,[paramnode(0),paramnode(1)]),
#                                                             constnode("")
#                                                                          ]),
#                                                         node(orW, [node(getValueW, [paramnode(0),paramnode(3)]),
#                                                                    node(getValueW, [paramnode(0),paramnode(4)])
#                                                                    ])
#                                                         ]),
#                                             constnode("Send for Clarification"),
#                                             node(ifW, [node(andW, [node(isEqualW, [node(getValueW, [paramnode(0),paramnode(1)]),
#                                                                                    constnode("Y")
#                                                                                    ]),
#                                                                    node(getValueW, [paramnode(0),paramnode(5)])
#                                                                    ]),
#                                                        constnode("L"),
#                                                        constnode("C")
#                                                        ])
#                                             ]),
#
#                                  constnode("")
#                                  ]),
#                       constnode("N")
#                       ])
# def var_board7():{Mappings.json"name":  "var_board7", "type": "tree", "input": ["masterDict['singleValue']['Board_Certified']", "'VAR_BOARD6'"], "col": "VAR_BOARD7"},
#     return node(ifW,[node(getValueW [paramnode(0),paramnode(1)]),
#                      node(ifW, [paramnode(0),paramnode(2)]),
#                      constnode("E")
#                 ])
def var_board8():
    return node(ifW, [node(getValueW, [paramnode(0),paramnode(1)]),
                      node(ifW, [node(isEqualW, [node(getValueW, [paramnode(0),paramnode(2)]),
                                      constnode("N")
                                                 ]),
                                 constnode("N"),
                                 node(ifW, [node(andW, [node(getValueW, [paramnode(0),paramnode(3)]),
                                                        node(getValueW, [paramnode(0),paramnode(4)])
                                                        ]),
                                            constnode("L"),
                                            node(ifW, [node(andW,
                                                            [node(getValueW, [paramnode(0),paramnode(5)]),
                                                             node(getValueW, [paramnode(0),paramnode(4)])]),
                                                       constnode("C"),
                                                       constnode("N")
                                                            ]),
                                            ])
                                            ]),
                      constnode("X")
                                 ])


# def var_board9():{"name": "var_board9", "type": "tree", "input": ["'VAR_BOARD6'", "'VAR_BOARD7'", "'VAR_BOARD8'"], "col": "VAR_BOARD9"}
#     return node(ifW, [node(isNotEmptyW, [paramnode(0),paramnode(1)]),
#                       node(getValueW, [paramnode(0),paramnode(1)]),
#                       node(ifW, [node(isNotEmptyW, [paramnode(0),paramnode(2)]),
#                                  node(getValueW, [paramnode(0),paramnode(2)]),
#                                  node(ifW, [node(isNotEmptyW, [paramnode(0),paramnode(3)]),
#                                             node(getValueW, [paramnode(0),paramnode(3)]),
#                                             constnode("")
#                                             ])
#                                 ])
#                 ])
def loc():
    return node(ifW, [node(isNotEmptyW, [paramnode(0),paramnode(3)]),
                      node(ifW, [node(isNotEmptyW, [paramnode(0), paramnode(1)]),
                                 node(getValueW, [paramnode(0), paramnode(1)]),
                                 node(getValueW, [paramnode(0), paramnode(2)])
                                 ]),
                      constnode("")

    ])

#########################################Medicaid###############################################
def individual_medicaid():
    return node(ifW, [node(isNotEmptyW, [paramnode(0),paramnode(1)]),
                      node(getValueW, [paramnode(0),paramnode(1)]),
                      constnode(getValueW, [paramnode(0),paramnode(2)])
    ])

######################################### Age limit################################################
def age_limit():
    return node(ifW, [
        node(isNotEmptyW, [paramnode(0),paramnode(1)]),
        node(getValueW, [paramnode(0),paramnode(1)]),
        constnode("")
    ])
# def min_age():
#     return node(ifW, [node(isNotEmptyW, [paramnode(0),paramnode(1)]),
#                       node(ifW, [node(isNotEmptyW, [paramnode(0), paramnode(2)]),
#                                  node(getValueW, [paramnode(0), paramnode(2)]),
#########################################DIR IND###############################################
def dir_ind():
    return node(ifW, [node(isNotEmptyW, [paramnode(0),paramnode(1)]),
                      node(getValueW, [paramnode(0),paramnode(1)]),
                      constnode("")
                      ])