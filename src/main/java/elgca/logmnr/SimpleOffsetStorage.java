package elgca.logmnr;

public class SimpleOffsetStorage extends OffsetStorage {
    private long commitscn = -1;
    private long earliestscn = 1;

    public SimpleOffsetStorage() {
    }

    @Override
    public long getCommitScn() {
        return commitscn;
    }

    @Override
    public long getEarliestScn() {
        return earliestscn;
    }

    @Override
    public void setCommitScn(Long scn) {
        commitscn = scn;
    }

    @Override
    public void setEarliestScn(Long scn) {
        earliestscn = scn;
    }
}
