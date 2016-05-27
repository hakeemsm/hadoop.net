using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Record.Compiler
{
	/// <summary>CodeGenerator is a Factory and a base class for Hadoop Record I/O translators.
	/// 	</summary>
	/// <remarks>
	/// CodeGenerator is a Factory and a base class for Hadoop Record I/O translators.
	/// Different translators register creation methods with this factory.
	/// </remarks>
	internal abstract class CodeGenerator
	{
		private static Dictionary<string, CodeGenerator> generators = new Dictionary<string
			, CodeGenerator>();

		static CodeGenerator()
		{
			Register("c", new CGenerator());
			Register("c++", new CppGenerator());
			Register("java", new JavaGenerator());
		}

		internal static void Register(string lang, CodeGenerator gen)
		{
			generators[lang] = gen;
		}

		internal static CodeGenerator Get(string lang)
		{
			return generators[lang];
		}

		/// <exception cref="System.IO.IOException"/>
		internal abstract void GenCode(string file, AList<JFile> inclFiles, AList<JRecord
			> records, string destDir, AList<string> options);
	}
}
