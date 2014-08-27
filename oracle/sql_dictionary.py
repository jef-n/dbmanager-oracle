# -*- coding: utf-8 -*-

"""
/***************************************************************************
Name                 : DB Manager
Description          : Database manager plugin for QGIS (Oracle)
Date                 : Aug 27, 2014
copyright            : (C) 2014 by Médéric RIBREUX
email                : mederic.ribreux@gmail.com

The content of this file is based on
- PG_Manager by Martin Dobias <wonder.sk@gmail.com> (GPLv2 license)
- DB Manager by Giuseppe Sucameli <brush.tyler@gmail.com> (GPLv2 license)
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
"""

__author__ = 'Médéric RIBREUX'
__date__ = 'August 2014'
__copyright__ = '(C) 2014, Médéric RIBREUX'
# This will get replaced with a git SHA1 when you do a git archive
__revision__ = '$Format:%H$'

# keywords
keywords = [
# From http://docs.oracle.com/cd/B19306_01/server.102/b14200/ap_keywd.htm
"access","add","all","alter","and","any","as","asc","audit","between","by",
"char","check","cluster","column","comment","compress","connect","create",
"current","date","decimal","default","delete","desc","distinct","drop",
"else","exclusive","exists","file","float","for","from","grant","group",
"having","identified","immediate","in","increment","index","initial","insert",
"integer","intersect","into","is","level","like","lock","long","maxextents",
"minus","mlslabel","mode","modify","noaudit","nocompress","not","nowait","null",
"number","of","offline","on","online","option","or","order","pctfree","prior",
"privileges","public","raw","rename","resource","revoke","row","rowid","rownum",
"rows","select","session","set","share","size","smallint","start","successful",
"synonym","sysdate","table","then","to","trigger","uid","union","unique",
"update","user","validate","values","varchar","varchar2","view","whenever",
"where","with",
# From http://docs.oracle.com/cd/B13789_01/appdev.101/a42525/apb.htm
"admin","cursor","found","mount","after","cycle","function","next","allocate",
"database","go","new","analyze","datafile","goto","noarchivelog","archive",
"dba","groups","nocache","archivelog","dec","including","nocycle","authorization",
"declare","indicator","nomaxvalue","avg","disable","initrans","nominvalue","backup",
"dismount","instance","none","begin","double","int","noorder","become","dump","key",
"noresetlogs","before","each","language","normal","block","enable","layer","nosort",
"body","end","link","numeric","cache","escape","lists","off","cancel","events",
"logfile","old","cascade","except","manage","only","change","exceptions","manual",
"open","character","exec","max","optimal","checkpoint","explain","maxdatafiles",
"own","close","execute","maxinstances","package","cobol","extent","maxlogfiles",
"parallel","commit","externally","maxloghistory","pctincrease","compile","fetch",
"maxlogmembers","pctused","constraint","flush","maxtrans","plan","constraints",
"freelist","maxvalue","pli","contents","freelists","min","precision","continue",
"force","minextents","primary","controlfile","foreign","minvalue","private","count",
"fortran","module","procedure","profile","savepoint","sqlstate","tracing","quota","schema",
"statement_id","transaction","read","scn","statistics","triggers","real","section","stop",
"truncate","recover","segment","storage","under","references","sequence","sum","unlimited",
"referencing","shared","switch","until","resetlogs","snapshot","system","use","restricted",
"some","tables","using","reuse","sort","tablespace","when","role","sql","temporary",
"write","roles","sqlcode","thread","work","rollback","sqlerror","time","abort","between",
"crash","digits","accept","binary_integer","create","dispose","access","body","current",
"distinct","add","boolean","currval","do","all","by","cursor","drop","alter","case",
"database","else","and","char","data_base","elsif","any","char_base","date","end","array",
"check","dba","entry","arraylen","close","debugoff","exception","as","cluster","debugon",
"exception_init","asc","clusters","declare","exists","assert","colauth","decimal","exit",
"assign","columns","default","false","at","commit","definition","fetch","authorization",
"compress","delay","float","avg","connect","delete","for","base_table","constant","delta",
"form","begin","count","desc","from","function","new","release","sum","generic","nextval",
"remr","tabauth","goto","nocompress","rename","table","grant","not","resource","tables",
"group","null","return","task","having","number","reverse","terminate","identified",
"number_base","revoke","then","if","of","rollback","to","in","on","rowid","true","index",
"open","rowlabel","type","indexes","option","rownum","union","indicator","or","rowtype",
"unique","insert","order","run","update","integer","others","savepoint","use","intersect",
"out","schema","values","into","package","select","varchar","is","partition","separate",
"varchar2","level","pctfree","set","variance","like","positive","size","view","limited",
"pragma","smallint","views","loop","prior","space","when","max","private","sql","where",
"min","procedure","sqlcode","while","minus","public","sqlerrm","with","mlslabel","raise",
"start","work","mod","range","statement","xor","mode","real","stddev","natural","record","subtype"
]
oracle_spatial_keywords = []

# functions
functions = [
# FROM https://docs.oracle.com/cd/B19306_01/server.102/b14200/functions001.htm
"abs","acos","add_months","appendchildxml","ascii","asciistr","asin","atan","atan2","avg",
"bfilename","bin_to_num","bitand","cardinality","cast","ceil","chartorowid","chr","cluster_id",
"cluster_probability","cluster_set","coalesce","collect","compose","concat","convert","corr",
"cos","cosh","count","covar_pop","covar_samp","cume_dist","current_date","current_timestamp",
"dbtimezone","decode","decompose","deletexml","dense_rank","depth","dump","empty_blob","empty_clob",
"existsnode","exp","extract","extractvalue","feature_id","feature_set","feature_value","first",
"floor","from_tz","greatest","grouping","grouping_id","group_id","hextoraw","initcap",
"insertchildxml","insertxmlbefore","instr","last","last_day","least","length","ln","lnnvl",
"localtimestamp","log","lower","lpad","ltrim","max","median","min","mod","months_between","nanvl",
"new_time","next_day","nlssort","nls_charset_decl_len","nls_charset_id","nls_charset_name","nls_initcap",
"nls_lower","nls_upper","nullif","numtodsinterval","nvl","nvl2","ora_hash","path","percentile_cont",
"percentile_disc","percent_rank","power","powermultiset","powermultiset_by_cardinality","prediction",
"prediction_cost","prediction_details","prediction_probability","prediction_set","rank","rawtohex",
"rawtonhex","regexp_instr","regexp_replace","regexp_substr","remainder","replace","round","round",
"rowidtochar","rowidtonchar","rpad","rtrim","scn_to_timestamp","sessiontimezone","set","sign","sin",
"sinh","soundex","sqrt","stats_binomial_test","stats_crosstab","stats_f_test","stats_ks_test",
"stats_mode","stats_mw_test","stats_one_way_anova","stats_wsr_test","stddev","stddev_pop","stddev_samp",
"substr","sum","sysdate","systimestamp","sys_connect_by_path","sys_context","sys_dburigen",
"sys_extract_utc","sys_guid","sys_typeid","sys_xmlagg","sys_xmlgen","tan","tanh","timestamp_to_scn",
"to_binary_double","to_binary_float","to_char","to_clob","to_date","to_dsinterval","to_lob",
"to_multi_byte","to_nchar","to_nclob","to_number","to_single_byte","to_timestamp","to_timestamp_tz",
"to_yminterval","translate","treat","trim","trunc","tz_offset","uid","unistr","updatexml","upper",
"user","userenv","variance","var_pop","var_samp","vsize","width_bucket","xmlagg","xmlcdata",
"xmlcolattval","xmlcomment","xmlconcat","xmlforest","xmlparse","xmlpi","xmlquery","xmlroot",
"xmlsequence","xmlserialize","xmltable","xmltransform" 
]
oracle_spatial_functions = [
# From http://docs.oracle.com/cd/B19306_01/appdev.102/b14255/toc.htm
## Spatial operators
"sdo_anyinteract","sdo_contains","sdo_coveredby","sdo_covers","sdo_equal","sdo_filter","sdo_inside","sdo_join","sdo_nn","sdo_nn_distance","sdo_on","sdo_overlapbdydisjoint","sdo_overlapbdyintersect","sdo_overlaps","sdo_relate","sdo_touch","sdo_within_distance",
## Spatial aggregate functions
"sdo_aggr_centroid","sdo_aggr_concat_lines","sdo_aggr_convexhull","sdo_aggr_lrs_concat","sdo_aggr_mbr","sdo_aggr_union",
## Coordinate system transformation (SDO_CS)
"sdo_cs.add_preference_for_op","sdo_cs.convert_nadcon_to_xml","sdo_cs.convert_ntv2_to_xml","sdo_cs.convert_xml_to_nadcon","sdo_cs.convert_xml_to_ntv2","sdo_cs.create_concatenated_op","sdo_cs.create_obvious_epsg_rules","sdo_cs.create_pref_concatenated_op","sdo_cs.delete_all_epsg_rules","sdo_cs.delete_op","sdo_cs.determine_chain","sdo_cs.determine_default_chain","sdo_cs.find_geog_crs","sdo_cs.find_proj_crs","sdo_cs.from_ogc_simplefeature_srs","sdo_cs.from_usng","sdo_cs.map_epsg_srid_to_oracle","sdo_cs.map_oracle_srid_to_epsg","sdo_cs.revoke_preference_for_op","sdo_cs.to_ogc_simplefeature_srs","sdo_cs.to_usng","sdo_cs.transform","sdo_cs.transform_layer","sdo_cs.update_wkts_for_all_epsg_crs","sdo_cs.update_wkts_for_epsg_crs","sdo_cs.update_wkts_for_epsg_datum","sdo_cs.update_wkts_for_epsg_ellips","sdo_cs.update_wkts_for_epsg_op","sdo_cs.update_wkts_for_epsg_param","sdo_cs.update_wkts_for_epsg_pm","sdo_cs.validate_wkt","sdo_cs.viewport_transform",
## Geocoding (SDO_GCDR)
"sdo_gcdr.geocode","sdo_gcdr.geocode_addr","sdo_gcdr.geocode_addr_all","sdo_gcdr.geocode_all","sdo_gcdr.geocode_as_geometry","sdo_gcdr.reverse_geocode",
## Geometry (SDO_GEOM)
"sdo_geom.relate","sdo_geom.sdo_arc_densify","sdo_geom.sdo_area","sdo_geom.sdo_buffer","sdo_geom.sdo_centroid","sdo_geom.sdo_convexhull","sdo_geom.sdo_difference","sdo_geom.sdo_distance","sdo_geom.sdo_intersection","sdo_geom.sdo_length","sdo_geom.sdo_max_mbr_ordinate","sdo_geom.sdo_mbr","sdo_geom.sdo_min_mbr_ordinate","sdo_geom.sdo_pointonsurface","sdo_geom.sdo_union","sdo_geom.sdo_xor","sdo_geom.validate_geometry_with_context","sdo_geom.validate_layer_with_context","sdo_geom.within_distance",
## Linear Referencing System (SDO_LRS)
"sdo_lrs.clip_geom_segment","sdo_lrs.concatenate_geom_segments","sdo_lrs.connected_geom_segments","sdo_lrs.convert_to_lrs_dim_array","sdo_lrs.convert_to_lrs_geom","sdo_lrs.convert_to_lrs_layer","sdo_lrs.convert_to_std_dim_array","sdo_lrs.convert_to_std_geom","sdo_lrs.convert_to_std_layer","sdo_lrs.define_geom_segment","sdo_lrs.dynamic_segment","sdo_lrs.find_lrs_dim_pos","sdo_lrs.find_measure","sdo_lrs.find_offset","sdo_lrs.geom_segment_end_measure","sdo_lrs.geom_segment_end_pt","sdo_lrs.geom_segment_length","sdo_lrs.geom_segment_start_measure","sdo_lrs.geom_segment_start_pt","sdo_lrs.get_measure","sdo_lrs.get_next_shape_pt","sdo_lrs.get_next_shape_pt_measure","sdo_lrs.get_prev_shape_pt","sdo_lrs.get_prev_shape_pt_measure","sdo_lrs.is_geom_segment_defined","sdo_lrs.is_measure_decreasing","sdo_lrs.is_measure_increasing","sdo_lrs.is_shape_pt_measure","sdo_lrs.locate_pt","sdo_lrs.lrs_intersection","sdo_lrs.measure_range","sdo_lrs.measure_to_percentage","sdo_lrs.offset_geom_segment","sdo_lrs.percentage_to_measure","sdo_lrs.project_pt","sdo_lrs.redefine_geom_segment","sdo_lrs.reset_measure","sdo_lrs.reverse_geometry","sdo_lrs.reverse_measure","sdo_lrs.set_pt_measure","sdo_lrs.split_geom_segment","sdo_lrs.translate_measure","sdo_lrs.valid_geom_segment","sdo_lrs.valid_lrs_pt","sdo_lrs.valid_measure","sdo_lrs.validate_lrs_geometry",
## SDO_MIGRATE
"sdo_migrate.to_current",
## Spatial Analysis and Mining (SDO_SAM)
"sdo_sam.aggregates_for_geometry","sdo_sam.aggregates_for_layer","sdo_sam.bin_geometry","sdo_sam.bin_layer","sdo_sam.colocated_reference_features","sdo_sam.simplify_geometry","sdo_sam.simplify_layer","sdo_sam.spatial_clusters","sdo_sam.tiled_aggregates","sdo_sam.tiled_bins",
## Tuning (SDO_TUNE)
"sdo_tune.average_mbr","sdo_tune.estimate_rtree_index_size","sdo_tune.extent_of","sdo_tune.mix_info","sdo_tune.quality_degradation",
## Utility (SDO_UTIL)
"sdo_util.append","sdo_util.circle_polygon","sdo_util.concat_lines","sdo_util.convert_unit","sdo_util.ellipse_polygon","sdo_util.extract","sdo_util.from_wkbgeometry","sdo_util.from_wktgeometry","sdo_util.getnumelem","sdo_util.getnumvertices","sdo_util.getvertices","sdo_util.initialize_indexes_for_tts","sdo_util.point_at_bearing","sdo_util.polygontoline","sdo_util.prepare_for_tts","sdo_util.rectify_geometry","sdo_util.remove_duplicate_vertices","sdo_util.reverse_linestring","sdo_util.simplify","sdo_util.to_gmlgeometry","sdo_util.to_wkbgeometry","sdo_util.to_wktgeometry","sdo_util.validate_wkbgeometry","sdo_util.validate_wktgeometry"
]

# constants
constants = [ "null", "false", "true" ]
oracle_spatial_constants = []

def getSqlDictionary(spatial=True):
	k, c, f = list(keywords), list(constants), list(functions)

	if spatial:
		k += oracle_spatial_keywords
		f += oracle_spatial_functions
		c += oracle_spatial_constants

	return { 'keyword' : k, 'constant' : c, 'function' : f }

