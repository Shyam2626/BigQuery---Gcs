import java.util.*;

class TestClass {

    static class TreeNode {
        String name;
        boolean isLocked;
        int id, anc_locked, des_locked, lockedDescendantsCount;
        TreeNode parent;

        List<TreeNode> child = new ArrayList<>();

        TreeNode(String name, TreeNode parent) {
            this.name = name;
            this.parent = parent;
            this.lockedDescendantsCount = 0;
        }
    }

    public static void main(String[] args) throws Exception {
        Scanner sc = new Scanner(System.in);

        int n = sc.nextInt();
       int m = sc.nextInt();
       int q = sc.nextInt();
       sc.nextLine();

       String[] nodes = new String[n];
       String[] queries = new String[q];

       for(int i = 0 ; i < n ; i++)
        nodes[i] = (sc.nextLine().trim());

        for(int i = 0 ; i < q ; i++)
            queries[i] = (sc.nextLine().trim());

        Map<String, TreeNode> map = new HashMap<>();

        TreeNode root = new TreeNode(nodes[0], null);
        map.put(nodes[0], root);

        Queue<TreeNode> queue = new LinkedList<>();
        queue.add(root);
        int idx = 1;

        while (queue.size() > 0 && idx < n) {
            int size = queue.size();

            while (size-- > 0) {
                TreeNode remove = queue.remove();

                for (int i = 1; i <= m && idx < n; i++) {
                    TreeNode node = new TreeNode(nodes[idx], remove);
                    map.put(nodes[idx], node);
                    remove.child.add(node);
                    queue.add(node);
                    idx++;
                }
            }
        }

        boolean answer;

        for (String query : queries) {
            String parts[] = query.split(" ");
            if (parts[0].equals("1"))
                answer = lock(map.get(parts[1]), Integer.parseInt(parts[2]));
            else if (parts[0].equals("2"))
                answer = unlock(map.get(parts[1]), Integer.parseInt(parts[2]));
            else
                answer = upgrade(map.get(parts[1]), Integer.parseInt(parts[2]));

            System.out.println(answer);
        }
    }

    static boolean lock(TreeNode node, int id) {
        if (node.isLocked)
            return false;

        if (node.anc_locked > 0 || node.lockedDescendantsCount > 0)
            return false;

        TreeNode cur = node.parent;

        while (cur != null) {
            cur.lockedDescendantsCount += 1;
            cur = cur.parent;
        }

        // informDescendant(node, 1);
        node.isLocked = true;
        node.id = id;
        return true;
    }

    // static void informDescendant(TreeNode node, int val){

    //     if(node == null)
    //         return;

    //     node.anc_locked += val;
    //     for(TreeNode des : node.child)
    //         informDescendant(des, val);

    // }

    static boolean unlock(TreeNode node, int id) {
        if (!node.isLocked || node.id != id)
            return false;

        TreeNode cur = node.parent;

        while (cur != null) {
            cur.lockedDescendantsCount -= 1;
            cur = cur.parent;
        }

        // informDescendant(node, -1);

        node.isLocked = false;
        node.id = 0;
        return true;
    }

    static boolean upgrade(TreeNode node, int id) {
        if (node.isLocked || node.anc_locked > 0 || node.lockedDescendantsCount == 0)
            return false;

        List<TreeNode> child = new ArrayList<>();
        boolean res = getAllChild(node, child, id);

        if (!res)
            return false;

        for (TreeNode k : child) {
            unlock(k, id);
        }

        node.isLocked = true;
        node.id = id;
        return true;
    }

    static boolean getAllChild(TreeNode node, List<TreeNode> child, int id) {
        if (node == null)
            return true;

        if (node.isLocked) {
            if (node.id != id)
                return false;
            else
                child.add(node);
        }

        for (TreeNode k : node.child) {
            boolean res = getAllChild(k, child, id);
            if (!res)
                return false;
        }
        return true;
    }
}
