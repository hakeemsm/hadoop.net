using Sharpen;

namespace org.apache.hadoop.record.compiler
{
	/// <summary>CodeGenerator is a Factory and a base class for Hadoop Record I/O translators.
	/// 	</summary>
	/// <remarks>
	/// CodeGenerator is a Factory and a base class for Hadoop Record I/O translators.
	/// Different translators register creation methods with this factory.
	/// </remarks>
	internal abstract class CodeGenerator
	{
		private static System.Collections.Generic.Dictionary<string, org.apache.hadoop.record.compiler.CodeGenerator
			> generators = new System.Collections.Generic.Dictionary<string, org.apache.hadoop.record.compiler.CodeGenerator
			>();

		static CodeGenerator()
		{
			register("c", new org.apache.hadoop.record.compiler.CGenerator());
			register("c++", new org.apache.hadoop.record.compiler.CppGenerator());
			register("java", new org.apache.hadoop.record.compiler.JavaGenerator());
		}

		internal static void register(string lang, org.apache.hadoop.record.compiler.CodeGenerator
			 gen)
		{
			generators[lang] = gen;
		}

		internal static org.apache.hadoop.record.compiler.CodeGenerator get(string lang)
		{
			return generators[lang];
		}

		/// <exception cref="System.IO.IOException"/>
		internal abstract void genCode(string file, System.Collections.Generic.List<org.apache.hadoop.record.compiler.JFile
			> inclFiles, System.Collections.Generic.List<org.apache.hadoop.record.compiler.JRecord
			> records, string destDir, System.Collections.Generic.List<string> options);
	}
}
