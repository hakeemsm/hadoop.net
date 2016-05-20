using Sharpen;

namespace org.apache.hadoop.record.compiler
{
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class JString : org.apache.hadoop.record.compiler.JCompType
	{
		internal class JavaString : org.apache.hadoop.record.compiler.JCompType.JavaCompType
		{
			internal JavaString(JString _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string getTypeIDObjectString()
			{
				return "org.apache.hadoop.record.meta.TypeID.StringTypeID";
			}

			internal override void genSlurpBytes(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, string b, string s, string l)
			{
				cb.append("{\n");
				cb.append("int i = org.apache.hadoop.record.Utils.readVInt(" + b + ", " + s + ");\n"
					);
				cb.append("int z = org.apache.hadoop.record.Utils.getVIntSize(i);\n");
				cb.append(s + "+=(z+i); " + l + "-= (z+i);\n");
				cb.append("}\n");
			}

			internal override void genCompareBytes(org.apache.hadoop.record.compiler.CodeBuffer
				 cb)
			{
				cb.append("{\n");
				cb.append("int i1 = org.apache.hadoop.record.Utils.readVInt(b1, s1);\n");
				cb.append("int i2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);\n");
				cb.append("int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);\n");
				cb.append("int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);\n");
				cb.append("s1+=z1; s2+=z2; l1-=z1; l2-=z2;\n");
				cb.append("int r1 = org.apache.hadoop.record.Utils.compareBytes(b1,s1,i1,b2,s2,i2);\n"
					);
				cb.append("if (r1 != 0) { return (r1<0)?-1:0; }\n");
				cb.append("s1+=i1; s2+=i2; l1-=i1; l1-=i2;\n");
				cb.append("}\n");
			}

			internal override void genClone(org.apache.hadoop.record.compiler.CodeBuffer cb, 
				string fname)
			{
				cb.append(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "other." + fname 
					+ " = this." + fname + ";\n");
			}

			private readonly JString _enclosing;
		}

		internal class CppString : org.apache.hadoop.record.compiler.JCompType.CppCompType
		{
			internal CppString(JString _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string getTypeIDObjectString()
			{
				return "new ::hadoop::TypeID(::hadoop::RIOTYPE_STRING)";
			}

			private readonly JString _enclosing;
		}

		/// <summary>Creates a new instance of JString</summary>
		public JString()
		{
			setJavaType(new org.apache.hadoop.record.compiler.JString.JavaString(this));
			setCppType(new org.apache.hadoop.record.compiler.JString.CppString(this));
			setCType(new org.apache.hadoop.record.compiler.JCompType.CCompType(this));
		}

		internal override string getSignature()
		{
			return "s";
		}
	}
}
