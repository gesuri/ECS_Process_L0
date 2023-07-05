# -------------------------------------------------------------------------------
# Name:        EddyCovarinceSystem Process for L0 Data
# Purpose:     take the files from loggernet, change de file name with the
#              current timestamp. Then the releated stored tables are updated
#              with the current file.
#
# Version:     0.1
#
# Author:      Gesuri Ramirez
#
# Created:     06/13/2023
# Copyright:   (c) Gesuri 2023
# Licence:     Apache 2.0
# -------------------------------------------------------------------------------

# info about CS tables:
# https://help.campbellsci.com/GRANITE9-10/Content/shared/Details/Data/About_Data_Tables.htm?TocPath=Working%20with%20data%7C_____4


from pathlib import Path
