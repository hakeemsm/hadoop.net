

namespace Org.Apache.Hadoop.Record.Compiler
{
	/// <summary>Code generator for "int" type</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class JInt : JType
	{
		internal class JavaInt : JType.JavaType
		{
			internal JavaInt(JInt _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string GetTypeIDObjectString()
			{
				return "org.apache.hadoop.record.meta.TypeID.IntTypeID";
			}

			internal override void GenSlurpBytes(CodeBuffer cb, string b, string s, string l)
			{
				cb.Append("{\n");
				cb.Append("int i = org.apache.hadoop.record.Utils.readVInt(" + b + ", " + s + ");\n"
					);
				cb.Append("int z = org.apache.hadoop.record.Utils.getVIntSize(i);\n");
				cb.Append(s + "+=z; " + l + "-=z;\n");
				cb.Append("}\n");
			}

			internal override void GenCompareBytes(CodeBuffer cb)
			{
				cb.Append("{\n");
				cb.Append("int i1 = org.apache.hadoop.record.Utils.readVInt(b1, s1);\n");
				cb.Append("int i2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);\n");
				cb.Append("if (i1 != i2) {\n");
				cb.Append("return ((i1-i2) < 0) ? -1 : 0;\n");
				cb.Append("}\n");
				cb.Append("int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);\n");
				cb.Append("int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);\n");
				cb.Append("s1+=z1; s2+=z2; l1-=z1; l2-=z2;\n");
				cb.Append("}\n");
			}

			private readonly JInt _enclosing;
		}

		internal class CppInt : JType.CppType
		{
			internal CppInt(JInt _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string GetTypeIDObjectString()
			{
				return "new ::hadoop::TypeID(::hadoop::RIOTYPE_INT)";
			}

			private readonly JInt _enclosing;
		}

		/// <summary>Creates a new instance of JInt</summary>
		public JInt()
		{
			SetJavaType(new JInt.JavaInt(this));
			SetCppType(new JInt.CppInt(this));
			SetCType(new JType.CType(this));
		}

		internal override string GetSignature()
		{
			return "i";
		}
	}
}
