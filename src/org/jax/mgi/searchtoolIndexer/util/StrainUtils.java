package org.jax.mgi.searchtoolIndexer.util;

public class StrainUtils {
	// name of "temp table" for strains (really the alias used in withStrains)
	public static String strainTempTable = "selected_strains";
	
	// 'with' clause suitable for leading off a SQL statement, yields an alias
	// that has strains meeting three criteria:
	//   1. not private
	//   2. having at least one attribute other than Not Applicable and Not Specified
	//   3. name does not contain 'involves', 'either', ' and ', ' or '.
	public static String withStrains = "with " + strainTempTable + " as ("
		+ "select s._Strain_key "
		+ "from prb_strain s "
		+ "where s.private = 0 "
		+ "  and s.strain not ilike '%involves%' "
		+ "  and s.strain not ilike '%either%' "
		+ "  and s.strain not ilike '% and %' "
		+ "  and s.strain not ilike '% or %' "
		+ "  and exists (select 1 from voc_annot va, voc_term t "
		+ "    where va._AnnotType_key = 1009 "
		+ "      and va._Term_key = t._Term_key "
		+ "      and t.term != 'Not Applicable' "
		+ "      and t.term != 'Not Specified' "
		+ "      and va._Object_key = s._Strain_key) "
		+ ")";
	
	public static String getDocumentKey(String dbKey, String dataSet) {
		if ("Strain".equals(dataSet)) {
			return dataSet + dbKey;
		}
		return dbKey;
	}
}
