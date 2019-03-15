package elgca.logmnr;

abstract class OffsetStorage {

    abstract public long getCommitScn();

    abstract public long getEarliestScn();

    abstract public void setCommitScn(Long scn);

    abstract public void setEarliestScn(Long scn);

    @Override
    public String toString() {
        return "OffsetStorage{" +
                "commitScn=" + String.valueOf(getCommitScn()) +
                ", earliestScn=" + String.valueOf(getEarliestScn()) +
                '}';
    }
}
