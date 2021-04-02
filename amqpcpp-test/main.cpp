
#include <string>
#include <iostream>
#include <optional>

int main()
{ 
    std::optional<std::string> str = "Hello world\n";
    std::cout << str.value_or("No hello\n");
    return 0;
}
