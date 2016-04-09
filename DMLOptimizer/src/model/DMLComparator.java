package model;

import java.util.Comparator;

public class DMLComparator implements Comparator<DML>
{
    public int compare( DML x, DML y )
    {
        return x.id - y.id;
    }
}