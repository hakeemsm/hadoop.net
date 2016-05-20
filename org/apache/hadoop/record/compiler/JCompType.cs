using Sharpen;

namespace org.apache.hadoop.record.compiler
{
	/// <summary>
	/// Abstract base class for all the "compound" types such as ustring,
	/// buffer, vector, map, and record.
	/// </summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	internal abstract class JCompType : org.apache.hadoop.record.compiler.JType
	{
		internal abstract class JavaCompType : org.apache.hadoop.record.compiler.JType.JavaType
		{
			internal JavaCompType(JCompType _enclosing, string type, string suffix, string wrapper
				, string typeIDByteString)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override void genCompareTo(org.apache.hadoop.record.compiler.CodeBuffer 
				cb, string fname, string other)
			{
				cb.append(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret = " + fname 
					+ ".compareTo(" + other + ");\n");
			}

			internal override void genEquals(org.apache.hadoop.record.compiler.CodeBuffer cb, 
				string fname, string peer)
			{
				cb.append(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret = " + fname 
					+ ".equals(" + peer + ");\n");
			}

			internal override void genHashCode(org.apache.hadoop.record.compiler.CodeBuffer cb
				, string fname)
			{
				cb.append(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret = " + fname 
					+ ".hashCode();\n");
			}

			internal override void genClone(org.apache.hadoop.record.compiler.CodeBuffer cb, 
				string fname)
			{
				cb.append(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "other." + fname 
					+ " = (" + this.getType() + ") this." + fname + ".clone();\n");
			}

			private readonly JCompType _enclosing;
		}

		internal abstract class CppCompType : org.apache.hadoop.record.compiler.JType.CppType
		{
			internal CppCompType(JCompType _enclosing, string type)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override void genGetSet(org.apache.hadoop.record.compiler.CodeBuffer cb, 
				string fname)
			{
				cb.append("virtual const " + this.getType() + "& get" + org.apache.hadoop.record.compiler.JType
					.toCamelCase(fname) + "() const {\n");
				cb.append("return " + fname + ";\n");
				cb.append("}\n");
				cb.append("virtual " + this.getType() + "& get" + org.apache.hadoop.record.compiler.JType
					.toCamelCase(fname) + "() {\n");
				cb.append("return " + fname + ";\n");
				cb.append("}\n");
			}

			private readonly JCompType _enclosing;
		}

		internal class CCompType : org.apache.hadoop.record.compiler.JType.CType
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
