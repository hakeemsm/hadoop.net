using Sharpen;

namespace Org.Apache.Hadoop.Record.Compiler
{
	/// <summary>
	/// Abstract base class for all the "compound" types such as ustring,
	/// buffer, vector, map, and record.
	/// </summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	internal abstract class JCompType : JType
	{
		internal abstract class JavaCompType : JType.JavaType
		{
			internal JavaCompType(JCompType _enclosing, string type, string suffix, string wrapper
				, string typeIDByteString)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override void GenCompareTo(CodeBuffer cb, string fname, string other)
			{
				cb.Append(Consts.RioPrefix + "ret = " + fname + ".compareTo(" + other + ");\n");
			}

			internal override void GenEquals(CodeBuffer cb, string fname, string peer)
			{
				cb.Append(Consts.RioPrefix + "ret = " + fname + ".equals(" + peer + ");\n");
			}

			internal override void GenHashCode(CodeBuffer cb, string fname)
			{
				cb.Append(Consts.RioPrefix + "ret = " + fname + ".hashCode();\n");
			}

			internal override void GenClone(CodeBuffer cb, string fname)
			{
				cb.Append(Consts.RioPrefix + "other." + fname + " = (" + this.GetType() + ") this."
					 + fname + ".clone();\n");
			}

			private readonly JCompType _enclosing;
		}

		internal abstract class CppCompType : JType.CppType
		{
			internal CppCompType(JCompType _enclosing, string type)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override void GenGetSet(CodeBuffer cb, string fname)
			{
				cb.Append("virtual const " + this.GetType() + "& get" + JType.ToCamelCase(fname) 
					+ "() const {\n");
				cb.Append("return " + fname + ";\n");
				cb.Append("}\n");
				cb.Append("virtual " + this.GetType() + "& get" + JType.ToCamelCase(fname) + "() {\n"
					);
				cb.Append("return " + fname + ";\n");
				cb.Append("}\n");
			}

			private readonly JCompType _enclosing;
		}

		internal class CCompType : JType.CType
		{
			internal CCompType(JCompType _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly JCompType _enclosing;
		}
	}
}
