using Sharpen;

namespace org.apache.hadoop.record.compiler
{
	/// <summary>Code generator for "buffer" type.</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class JBuffer : org.apache.hadoop.record.compiler.JCompType
	{
		internal class JavaBuffer : org.apache.hadoop.record.compiler.JCompType.JavaCompType
		{
			internal JavaBuffer(JBuffer _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string getTypeIDObjectString()
			{
				return "org.apache.hadoop.record.meta.TypeID.BufferTypeID";
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

			internal override void genSlurpBytes(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, string b, string s, string l)
			{
				cb.append("{\n");
				cb.append("int i = org.apache.hadoop.record.Utils.readVInt(" + b + ", " + s + ");\n"
					);
				cb.append("int z = org.apache.hadoop.record.Utils.getVIntSize(i);\n");
				cb.append(s + " += z+i; " + l + " -= (z+i);\n");
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

			private readonly JBuffer _enclosing;
		}

		internal class CppBuffer : org.apache.hadoop.record.compiler.JCompType.CppCompType
		{
			internal CppBuffer(JBuffer _enclosing)
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

			internal override string getTypeIDObjectString()
			{
				return "new ::hadoop::TypeID(::hadoop::RIOTYPE_BUFFER)";
			}

			private readonly JBuffer _enclosing;
		}

		/// <summary>Creates a new instance of JBuffer</summary>
		public JBuffer()
		{
			setJavaType(new org.apache.hadoop.record.compiler.JBuffer.JavaBuffer(this));
			setCppType(new org.apache.hadoop.record.compiler.JBuffer.CppBuffer(this));
			setCType(new org.apache.hadoop.record.compiler.JCompType.CCompType(this));
		}

		internal override string getSignature()
		{
			return "B";
		}
	}
}
