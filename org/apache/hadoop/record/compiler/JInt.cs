using Sharpen;

namespace org.apache.hadoop.record.compiler
{
	/// <summary>Code generator for "int" type</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class JInt : org.apache.hadoop.record.compiler.JType
	{
		internal class JavaInt : org.apache.hadoop.record.compiler.JType.JavaType
		{
			internal JavaInt(JInt _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string getTypeIDObjectString()
			{
				return "org.apache.hadoop.record.meta.TypeID.IntTypeID";
			}

			internal override void genSlurpBytes(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, string b, string s, string l)
			{
				cb.append("{\n");
				cb.append("int i = org.apache.hadoop.record.Utils.readVInt(" + b + ", " + s + ");\n"
					);
				cb.append("int z = org.apache.hadoop.record.Utils.getVIntSize(i);\n");
				cb.append(s + "+=z; " + l + "-=z;\n");
				cb.append("}\n");
			}

			internal override void genCompareBytes(org.apache.hadoop.record.compiler.CodeBuffer
				 cb)
			{
				cb.append("{\n");
				cb.append("int i1 = org.apache.hadoop.record.Utils.readVInt(b1, s1);\n");
				cb.append("int i2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);\n");
				cb.append("if (i1 != i2) {\n");
				cb.append("return ((i1-i2) < 0) ? -1 : 0;\n");
				cb.append("}\n");
				cb.append("int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);\n");
				cb.append("int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);\n");
				cb.append("s1+=z1; s2+=z2; l1-=z1; l2-=z2;\n");
				cb.append("}\n");
			}

			private readonly JInt _enclosing;
		}

		internal class CppInt : org.apache.hadoop.record.compiler.JType.CppType
		{
			internal CppInt(JInt _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string getTypeIDObjectString()
			{
				return "new ::hadoop::TypeID(::hadoop::RIOTYPE_INT)";
			}

			private readonly JInt _enclosing;
		}

		/// <summary>Creates a new instance of JInt</summary>
		public JInt()
		{
			setJavaType(new org.apache.hadoop.record.compiler.JInt.JavaInt(this));
			setCppType(new org.apache.hadoop.record.compiler.JInt.CppInt(this));
			setCType(new org.apache.hadoop.record.compiler.JType.CType(this));
		}

		internal override string getSignature()
		{
			return "i";
		}
	}
}
