

namespace Org.Apache.Hadoop.Record.Compiler
{
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class JString : JCompType
	{
		internal class JavaString : JCompType.JavaCompType
		{
			internal JavaString(JString _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string GetTypeIDObjectString()
			{
				return "org.apache.hadoop.record.meta.TypeID.StringTypeID";
			}

			internal override void GenSlurpBytes(CodeBuffer cb, string b, string s, string l)
			{
				cb.Append("{\n");
				cb.Append("int i = org.apache.hadoop.record.Utils.readVInt(" + b + ", " + s + ");\n"
					);
				cb.Append("int z = org.apache.hadoop.record.Utils.getVIntSize(i);\n");
				cb.Append(s + "+=(z+i); " + l + "-= (z+i);\n");
				cb.Append("}\n");
			}

			internal override void GenCompareBytes(CodeBuffer cb)
			{
				cb.Append("{\n");
				cb.Append("int i1 = org.apache.hadoop.record.Utils.readVInt(b1, s1);\n");
				cb.Append("int i2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);\n");
				cb.Append("int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);\n");
				cb.Append("int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);\n");
				cb.Append("s1+=z1; s2+=z2; l1-=z1; l2-=z2;\n");
				cb.Append("int r1 = org.apache.hadoop.record.Utils.compareBytes(b1,s1,i1,b2,s2,i2);\n"
					);
				cb.Append("if (r1 != 0) { return (r1<0)?-1:0; }\n");
				cb.Append("s1+=i1; s2+=i2; l1-=i1; l1-=i2;\n");
				cb.Append("}\n");
			}

			internal override void GenClone(CodeBuffer cb, string fname)
			{
				cb.Append(Consts.RioPrefix + "other." + fname + " = this." + fname + ";\n");
			}

			private readonly JString _enclosing;
		}

		internal class CppString : JCompType.CppCompType
		{
			internal CppString(JString _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string GetTypeIDObjectString()
			{
				return "new ::hadoop::TypeID(::hadoop::RIOTYPE_STRING)";
			}

			private readonly JString _enclosing;
		}

		/// <summary>Creates a new instance of JString</summary>
		public JString()
		{
			SetJavaType(new JString.JavaString(this));
			SetCppType(new JString.CppString(this));
			SetCType(new JCompType.CCompType(this));
		}

		internal override string GetSignature()
		{
			return "s";
		}
	}
}
