package elgca.logmnr;

public abstract class OffsetStorage {

    abstract public long getCommitScn();

    abstract public long getEarliestScn();

    abstract public void setCommitScn(Long scn);

    abstract public void setEarliestScn(Long scn);

    @Override
    public String toString() {
        return "Offsets{" +
                "commitScn=" + String.valueOf(getCommitScn()) +
                ", earliestScn=" + String.valueOf(getEarliestScn()) +
                '}';
    }
}
