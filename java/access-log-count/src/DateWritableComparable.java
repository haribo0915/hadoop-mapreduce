import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class DateWritableComparable implements WritableComparable<DateWritableComparable>
{
	private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd' T 'HH:mm:ss.SSS");
	private Date date;
	
	public Date getDate() {
		return date;
	}
	
	public void setDate(Date date) {
		this.date = date;
	}
	
	// deserialize the fields of this object from in
	public void readFields(DataInput in) throws IOException {
		date = new Date(in.readLong());
	}
	
	// serialize the fields of this object to out
	public void write(DataOutput out) throws IOException {
		out.writeLong(date.getTime());
	}
	
	public String toString() {
		return sdf.format(date);
	}

    public int compareTo(DateWritableComparable otherDate) {
        return date.compareTo(otherDate.getDate());
    }
}